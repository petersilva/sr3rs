use clap::{Parser, Subcommand};
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow};
use sr3rs::config::paths;
use std::path::PathBuf;
use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Set log level (debug, info, warn, error)
    #[arg(long, global = true, alias = "logLevel")]
    log_level: Option<String>,

    /// Enable debug logging (alias for --log_level debug)
    #[arg(long, global = true)]
    debug: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Show the resolved configuration
    Show {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long, default_value = "flow")]
        component: String,

        /// Path to the configuration file
        config_file: PathBuf,
    },
    /// Run a flow
    Run {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long, default_value = "subscribe")]
        component: String,

        /// Path to the configuration file
        config_file: PathBuf,

        /// Run in background (daemonize)
        #[arg(short, long)]
        daemon: bool,

        /// Instance number
        #[arg(short, long, default_value = "1")]
        instance: u32,
    },
}

fn setup_logging(level: log::LevelFilter, log_file: Option<PathBuf>) -> Result<()> {
    let mut dispatch = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{}] {} {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%Z"),
                record.level(),
                record.target(),
                message
            ))
        })
        .level(level);

    if let Some(path) = log_file {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        dispatch = dispatch.chain(fern::log_file(path)?);
    } else {
        dispatch = dispatch.chain(std::io::stderr());
    }

    dispatch.apply()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let log_level = if cli.debug {
        log::LevelFilter::Debug
    } else {
        match cli.log_level.as_deref().unwrap_or("info").to_lowercase().as_str() {
            "debug" => log::LevelFilter::Debug,
            "info" => log::LevelFilter::Info,
            "warn" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        }
    };

    match cli.command {
        Commands::Show { component, config_file } => {
            setup_logging(log_level, None)?;
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            
            if let Some(path_str) = config_file.to_str() {
                config.load(path_str)?;
            }
            
            config.finalize()?;

            let json = serde_json::to_string_pretty(&config)?;
            println!("{}", json);
        }
        Commands::Run { component, config_file, daemon, instance } => {
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            
            if let Some(path_str) = config_file.to_str() {
                config.load(path_str)?;
            }
            
            config.finalize()?;

            let config_name = config.configname.clone();
            
            if daemon {
                let log_file = paths::get_log_filename(&component, config_name.as_deref(), instance);
                let pid_file = paths::get_pid_filename(&component, config_name.as_deref(), instance);

                setup_logging(log_level, Some(log_file.clone()))?;
                log::info!("Daemon mode active. Logging to {}, PID: {}", log_file.display(), std::process::id());

                if let Some(parent) = pid_file.parent() { std::fs::create_dir_all(parent)?; }
                let mut f = std::fs::File::create(pid_file)?;
                write!(f, "{}", std::process::id())?;
            } else {
                setup_logging(log_level, None)?;
            }

            let token = tokio_util::sync::CancellationToken::new();
            let token_clone = token.clone();

            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
                log::info!("Signal received, shutting down gracefully...");
                token_clone.cancel();
            });

            match component.as_str() {
                "subscribe" => {
                    let mut flow = SubscribeFlow::new(config);
                    flow.connect().await?;
                    log::info!("Connected to broker. Starting flow loop...");
                    flow.run_with_shutdown(token).await?;
                }
                _ => {
                    anyhow::bail!("Unsupported component for run: {}", component);
                }
            }
        }
    }

    Ok(())
}
