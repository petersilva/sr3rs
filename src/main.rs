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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logger based on CLI options or environment variable
    let mut builder = env_logger::Builder::from_default_env();
    
    let log_level = if cli.debug {
        Some("debug".to_string())
    } else {
        cli.log_level.clone()
    };

    if let Some(ref level_str) = log_level {
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

                println!("Daemonizing... log: {}, pid: {}", log_file.display(), pid_file.display());

                // Ensure directories exist
                if let Some(parent) = log_file.parent() { std::fs::create_dir_all(parent)?; }
                if let Some(parent) = pid_file.parent() { std::fs::create_dir_all(parent)?; }

                // Basic daemonization (manual for now to avoid extra complex dependencies)
                // In a production app, use 'daemonize' crate.
                let stdout = OpenOptions::new().create(true).append(true).open(&log_file)?;
                let stderr = stdout.try_clone()?;

                // For simplicity, we'll just redirect the current process if we were already 
                // intended to be backgrounded, but a real daemon would fork.
                // Redirecting log crate output if using env_logger:
                // We'd need to re-init logger to file or use a different logger.
                
                // Let's just write the PID file
                let mut f = std::fs::File::create(pid_file)?;
                write!(f, "{}", std::process::id())?;
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
