use aws_sdk_s3 as s3;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The R2 bucket to connect to.
    #[arg(short, long)]
    bucket: String,

    /// The R2 account ID.
    #[arg(short, long)]
    account_id: String,

    /// The R2 access key ID.
    #[arg(short, long)]
    key_id: String,

    /// The R2 access key secret.
    #[arg(short, long)]
    secret: String,
}

#[derive(Clone)]
struct ObjectStorage {
    bucket: Box<str>,
    client: s3::Client,
}

impl ObjectStorage {
    async fn new() -> Self {
        let args = Args::parse();

        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(format!(
                "https://{}.r2.cloudflarestorage.com",
                args.account_id
            ))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                args.key_id,
                args.secret,
                None,
                None,
                "R2",
            ))
            .region("auto")
            .load()
            .await;

        Self {
            bucket: args.bucket.into(),
            client: s3::Client::new(&config),
        }
    }

    pub async fn list_all_objects(&self) -> color_eyre::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token = None;

        let pb = indicatif::ProgressBar::new_spinner();
        pb.set_style(
            indicatif::ProgressStyle::with_template(
                "{spinner:.green} [{elapsed}] {pos} objects found",
            )
            .unwrap(),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        loop {
            let mut req = self.client.list_objects_v2().bucket(self.bucket.as_ref());

            if let Some(token) = &continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await?;

            if let Some(objects) = resp.contents {
                for obj in objects {
                    if let Some(key) = obj.key {
                        keys.push(key);
                        pb.inc(1);
                    }
                }
            }

            if resp.is_truncated == Some(true) {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        pb.finish_with_message(format!("Done. Found {} objects.", keys.len()));

        Ok(keys)
    }

    pub async fn get_object(&self, key: &str) -> color_eyre::Result<s3::primitives::ByteStream> {
        let resp = self
            .client
            .get_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await?;

        let stream = resp.body;

        Ok(stream)
    }
}

struct CompressObject {
    id: usize,
    key: String,
    storage: ObjectStorage,
}

enum WriteStrategy {
    File,
    R2,
}

pub async fn create_file_with_dirs(path_str: &str) -> color_eyre::Result<tokio::fs::File> {
    let path = std::path::Path::new(path_str);

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let file = tokio::fs::File::create(path).await?;
    Ok(file)
}

impl CompressObject {
    fn new(id: usize, key: &str, storage: ObjectStorage) -> Self {
        Self {
            id,
            key: key.to_owned(),
            storage,
        }
    }

    async fn run(self, strategy: WriteStrategy) -> color_eyre::Result<()> {
        let id = self.id;
        let key = self.key.as_str();
        tracing::info!("[worker {id}] processing {key}");

        let stream = self.storage.get_object(key).await?;

        let mut encoder =
            async_compression::tokio::bufread::BrotliEncoder::new(stream.into_async_read());

        match strategy {
            WriteStrategy::File => {
                let mut file = create_file_with_dirs(&format!("data/{}.br", self.key)).await?;
                tokio::io::copy(&mut encoder, &mut file).await?;
            }
            WriteStrategy::R2 => todo!(),
        }

        tracing::info!("[worker {id}] finished processing {key}");

        Ok(())
    }
}

async fn get_objects_channel(
    storage: &ObjectStorage,
) -> color_eyre::Result<(
    async_channel::Sender<String>,
    async_channel::Receiver<String>,
)> {
    tracing::info!("Fetching all object keys in bucket");
    let objects = storage.list_all_objects().await?;
    let (tx, rx) = async_channel::bounded::<String>(objects.len());

    for key in objects {
        tx.send(key).await?;
    }

    Ok((tx, rx))
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "tracing=info,warp=debug,r2squeeze=debug".to_owned());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let storage = ObjectStorage::new().await;
    let (tx, rx) = get_objects_channel(&storage).await?;

    let mut set = tokio::task::JoinSet::new();
    for id in 0..5 {
        let storage = ObjectStorage {
            bucket: storage.bucket.clone(),
            client: storage.client.clone(),
        };

        let tx = tx.clone();
        let rx = rx.clone();

        set.spawn(async move {
            while let Ok(key) = rx.recv().await {
                let storage = storage.clone();
                match CompressObject::new(id, key.as_str(), storage)
                    .run(WriteStrategy::File)
                    .await
                {
                    Ok(()) => {}
                    Err(err) => {
                        tracing::error!("[worker {id}] failed processing {key}: {err}");
                        let _ = tx.send(key).await;
                    }
                }
            }
        });
    }

    set.join_all().await;

    Ok(())
}
