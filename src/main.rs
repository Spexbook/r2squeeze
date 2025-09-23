use aws_sdk_s3 as s3;
use clap::Parser;

/// Compresses all objects in a given R2 bucket using Brotli.
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// The R2 bucket to connect to.
    #[arg(long)]
    bucket: String,

    /// The R2 account ID.
    #[arg(long)]
    account_id: String,

    /// The R2 access key ID.
    #[arg(long)]
    key_id: String,

    /// The R2 access key secret.
    #[arg(long)]
    secret: String,

    /// The strategy to when writing compressed files.
    #[arg(long)]
    strategy: WriteStrategy,

    /// The number of jobs to spawn.
    #[arg(long)]
    jobs: usize,
}

#[derive(Clone)]
enum WriteStrategy {
    File,
    R2,
}

impl std::str::FromStr for WriteStrategy {
    type Err = color_eyre::Report;

    fn from_str(s: &str) -> color_eyre::Result<Self> {
        match s {
            "file" => Ok(WriteStrategy::File),
            "r2" => Ok(WriteStrategy::R2),
            _ => color_eyre::eyre::bail!("unknown strategy type: {s}"),
        }
    }
}

#[derive(Clone)]
struct ObjectStorage {
    bucket: Box<str>,
    client: s3::Client,
}

impl ObjectStorage {
    async fn new(args: &Args) -> Self {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .endpoint_url(format!(
                "https://{}.r2.cloudflarestorage.com",
                args.account_id
            ))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                args.key_id.to_owned(),
                args.secret.to_owned(),
                None,
                None,
                "R2",
            ))
            .region("auto")
            .load()
            .await;

        Self {
            bucket: args.bucket.to_owned().into_boxed_str(),
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

    pub async fn put_object(
        &self,
        key: &str,
        stream: s3::primitives::ByteStream,
    ) -> color_eyre::Result<()> {
        self.client
            .put_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .body(stream)
            .send()
            .await?;

        Ok(())
    }

    pub async fn exists(&self, key: &str) -> color_eyre::Result<bool> {
        let resp = self
            .client
            .head_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await;

        match resp {
            Ok(_) => Ok(true),
            Err(err) if err.as_service_error().is_some_and(|x| x.is_not_found()) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn remove(&self, key: &str) -> color_eyre::Result<()> {
        self.client
            .delete_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await?;

        Ok(())
    }
}

struct CompressObject {
    id: usize,
    key: String,
    storage: ObjectStorage,
}

impl CompressObject {
    fn new(id: usize, key: &str, storage: ObjectStorage) -> Self {
        Self {
            id,
            key: key.to_owned(),
            storage,
        }
    }

    pub async fn create_file(&self) -> color_eyre::Result<tokio::fs::File> {
        let path = format!("data/{}.br", self.key);
        let path = std::path::Path::new(&path);

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        if tokio::fs::metadata(path).await.is_ok() {
            tokio::fs::remove_file(path).await?;
        }

        let file = tokio::fs::File::create(path).await?;
        Ok(file)
    }

    pub async fn create_object(
        &self,
        stream: s3::primitives::ByteStream,
    ) -> color_eyre::Result<()> {
        let id = self.id;
        let key = format!("{}.br", self.key);

        if self.storage.exists(&key).await? {
            tracing::info!("[worker {id}] deleting {key} from R2");
            self.storage.remove(&key).await?
        }

        self.storage.put_object(&key, stream).await?;

        Ok(())
    }

    async fn run(self, strategy: WriteStrategy) -> color_eyre::Result<()> {
        let id = self.id;
        let key = self.key.as_str();
        tracing::info!("[worker {id}] compressing {key}");

        let stream = self.storage.get_object(key).await?;

        let mut encoder =
            async_compression::tokio::bufread::BrotliEncoder::new(stream.into_async_read());

        match strategy {
            WriteStrategy::File => {
                let mut file = self.create_file().await?;
                tokio::io::copy(&mut encoder, &mut file).await?;
            }
            WriteStrategy::R2 => {
                let mut output = Vec::new();
                tokio::io::copy(&mut encoder, &mut output).await?;
                let stream = s3::primitives::ByteStream::from(output);
                self.create_object(stream).await?
            }
        }

        tracing::info!("[worker {id}] finished compressing {key}");

        Ok(())
    }
}

#[derive(Clone)]
struct ObjectsListCache {
    db: sled::Db,
}

impl ObjectsListCache {
    async fn new() -> color_eyre::Result<Self> {
        Ok(Self {
            db: sled::open("r2squeeze.sled")?,
        })
    }

    async fn empty(&self) -> color_eyre::Result<bool> {
        let db = self.db.clone();
        let length = tokio::task::spawn_blocking(move || db.len()).await?;

        Ok(length == 0)
    }

    async fn list(&self) -> color_eyre::Result<Vec<String>> {
        let db = self.db.clone();
        let objects: Vec<String> = tokio::task::spawn_blocking(move || {
            db.iter()
                .keys()
                .filter_map(|x| x.ok().map(|x| String::from_utf8_lossy(&*x).to_string()))
                .collect()
        })
        .await?;

        Ok(objects)
    }

    async fn insert(&self, key: &str) -> color_eyre::Result<()> {
        let db = self.db.clone();
        let key = key.to_owned();
        let _ = tokio::task::spawn_blocking(move || db.insert(key, b"")).await??;

        Ok(())
    }

    async fn remove(&self, key: &str) -> color_eyre::Result<()> {
        let db = self.db.clone();
        let key = key.to_owned();
        let _ = tokio::task::spawn_blocking(move || db.remove(key)).await??;

        Ok(())
    }
}

async fn get_objects_channel(
    storage: &ObjectStorage,
    cache: &ObjectsListCache,
) -> color_eyre::Result<(
    async_channel::Sender<String>,
    async_channel::Receiver<String>,
)> {
    let objects = if cache.empty().await? {
        tracing::info!("Fetching all object keys in bucket");
        let objects = storage.list_all_objects().await?;

        for key in objects.iter() {
            cache.insert(key).await?
        }

        objects
    } else {
        tracing::info!("Cache is not empty retrying cached files");
        cache.list().await?
    };

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

    let args = Args::parse();
    let storage = ObjectStorage::new(&args).await;
    let cache = ObjectsListCache::new().await?;

    let (tx, rx) = get_objects_channel(&storage, &cache).await?;

    let mut set = tokio::task::JoinSet::new();
    let strategy = args.strategy.clone();
    for id in 0..args.jobs {
        let strategy = strategy.clone();
        let storage = storage.clone();
        let cache = cache.clone();
        let tx = tx.clone();
        let rx = rx.clone();

        set.spawn(async move {
            while let Ok(key) = rx.recv().await {
                let storage = storage.clone();
                let cache = cache.clone();
                let strategy = strategy.clone();
                match CompressObject::new(id, key.as_str(), storage)
                    .run(strategy)
                    .await
                {
                    Ok(()) => {}
                    Err(err) => {
                        tracing::error!("[worker {id}] failed to compress {key}: {err}");
                        let res = cache.remove(&key).await;
                        tracing::info!("[worker {id}] remove from cache result {key}: {res:?}");
                        let _ = tx.send(key.to_owned()).await;
                        tracing::info!(
                            "[worker {id}] send retry to channel result: {key}: {res:?}"
                        );
                    }
                }
            }
        });
    }

    set.join_all().await;

    Ok(())
}
