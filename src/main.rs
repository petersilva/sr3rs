use clap::{Parser, Subcommand};
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow};
use std::path::PathBuf;
use anyhow::Result;

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
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logger based on CLI options or environment variable
    let mut builder = env_logger::Builder::from_default_env();
    
    if cli.debug {
        builder.filter_level(log::LevelFilter::Debug);
    } else if let Some(level_str) = &cli.log_level {
        let level = match level_str.to_lowercase().as_str() {
            "debug" => log::LevelFilter::Debug,
            "info" => log::LevelFilter::Info,
            "warn" => log::LevelFilter::Warn,
            "error" => log::LevelFilter::Error,
            _ => log::LevelFilter::Info,
        };
        builder.filter_level(level);
    }
    builder.init();

    match cli.command {
        Commands::Show { component, config_file } => {
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            
            if let Some(path_str) = config_file.to_str() {
                config.load(path_str)?;
            }
            
            config.finalize()?;

            let json = serde_json::to_string_pretty(&config)?;
            println!("{}", json);
        }
        Commands::Run { component, config_file } => {
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            
            if let Some(path_str) = config_file.to_str() {
                config.load(path_str)?;
            }
            
            config.finalize()?;

            let token = tokio_util::sync::CancellationToken::new();
            let token_clone = token.clone();

            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
                println!("\nCtrl+C received, shutting down gracefully...");
                token_clone.cancel();
            });

            match component.as_str() {
                "subscribe" => {
                    let mut flow = SubscribeFlow::new(config);
                    flow.connect().await?;
                    println!("Connected to broker. Starting flow loop...");
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
