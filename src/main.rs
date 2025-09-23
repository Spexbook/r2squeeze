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

    tracing::info!("Hello, world!");

    Ok(())
}
