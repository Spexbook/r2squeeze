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

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "tracing=info,warp=debug,r2squeeze=debug".to_owned());

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
        .init();

    let storage = ObjectStorage::new().await;

    tracing::info!("Fetching all object keys in bucket");
    let objects = storage.list_all_objects().await?;

    tracing::info!("Hello, world!");

    Ok(())
}
