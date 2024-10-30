#[cfg(feature = "sfcp")]
mod sfcp {
    use anyhow::{anyhow, Context};
    use clap::{Parser, Subcommand};
    use object_store::path::Path;
    use object_store_ffi::Compression;
    use std::path::{Path as OsPath, PathBuf};
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    #[derive(Parser, Debug)]
    #[command(name = "sfcp")]
    #[command(author, version, about = "Copy tool for Snowflake internal stages", long_about = None)]
    struct Cli {
        from: String,
        to: String,
        #[arg(short, long)]
        compression: Option<Compression>
    }

    #[derive(Debug)]
    enum Location {
        Stage(String, Path),
        File(PathBuf),
        Directory(PathBuf)
    }

    fn parse_arg(arg: &str, will_create: bool) -> anyhow::Result<Location> {
        if arg.starts_with("@") {
            let (stage, key) = arg
                .strip_prefix("@")
                .unwrap()
                .split_once('/')
                .context("Missing key of stage location")?;
            Ok(Location::Stage(stage.into(), Path::from(key)))
        } else {
            let ospath = std::path::Path::new(arg);
            if ospath.is_file() {
                Ok(Location::File(ospath.to_path_buf()))
            } else if ospath.is_dir() {
                Ok(Location::Directory(ospath.to_path_buf()))
            } else if will_create && !ospath.ends_with("/") {
                Ok(Location::File(ospath.to_path_buf()))
            } else {
                Err(anyhow!("The provided path `{arg}` does not point to a valid location"))
            }
        }
    }

    async fn setup_client(stage: &str) -> anyhow::Result<object_store_ffi::Client> {
        let config_map = serde_json::from_value(serde_json::json!({
            "url": format!("snowflake://{stage}"),
            "snowflake_stage": stage,
            "max_retries": "1000",
            "timeout": "40s",
            "connect_timeout": "40s",
            "retry_timeout_secs": "180"
        }))?;

        let client = object_store_ffi::Client::from_config_map(config_map).await?;
        Ok(client)
    }

    pub(crate) async fn main() -> anyhow::Result<()> {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "sfcp=info".into()),
            )
            .with(tracing_subscriber::fmt::layer())
            .init();

        let cli = Cli::parse();

        let from = parse_arg(cli.from.as_ref(), false)?;
        let to = parse_arg(cli.to.as_ref(), true)?;

        match (from, to) {
            (Location::File(from), Location::Stage(stage, to)) => {
                let client = setup_client(&stage).await?;
                client.upload(&from, &to, cli.compression.unwrap_or(Compression::None)).await?;
            }
            (Location::Stage(stage, from), Location::File(to)) => {
                let client = setup_client(&stage).await?;
                client.download(&from, &to, cli.compression.unwrap_or(Compression::None)).await?;
            }
            (Location::Directory(from), Location::Stage(stage, to)) => {
                let client = setup_client(&stage).await?;
                client.upload_directory(&from, &to, cli.compression.unwrap_or(Compression::None)).await?;
            }
            (Location::Stage(stage, from), Location::Directory(to)) => {
                let client = setup_client(&stage).await?;
                client.download_prefix(&from, &to, cli.compression.unwrap_or(Compression::None)).await?;
            }
            _ => {
                return Err(anyhow!("Invalid locations, one of the locations must be local and the other remote"))
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "sfcp")]
    return sfcp::main().await;

    Ok(())
}
