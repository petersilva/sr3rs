use clap::{Parser, Subcommand};
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow};
use sr3rs::config::paths;
use std::path::PathBuf;
use anyhow::Result;
use std::io::Write;
use std::process::Command;

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
    /// Run a flow in the foreground
    Foreground {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long, default_value = "subscribe")]
        component: String,

        /// Path to the configuration file
        config_file: PathBuf,
    },
    /// Start flow instances as daemons
    Start {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long, default_value = "subscribe")]
        component: String,

        /// Path to the configuration file
        config_file: PathBuf,
    },
    /// Stop flow instances
    Stop {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long, default_value = "subscribe")]
        component: String,

        /// Path to the configuration file
        config_file: PathBuf,
    },
    /// Internal command to run a specific daemon instance
    RunInstance {
        component: String,
        config_file: PathBuf,
        instance: u32,
    }
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
            if let Some(path_str) = config_file.to_str() { config.load(path_str)?; }
            config.finalize()?;
            let json = serde_json::to_string_pretty(&config)?;
            println!("{}", json);
        }
        Commands::Foreground { component, config_file } => {
            setup_logging(log_level, None)?;
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            if let Some(path_str) = config_file.to_str() { config.load(path_str)?; }
            config.finalize()?;

            let token = tokio_util::sync::CancellationToken::new();
            let token_clone = token.clone();
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
                log::info!("Ctrl+C received, shutting down gracefully...");
                token_clone.cancel();
            });

            match component.as_str() {
                "subscribe" => {
                    let mut flow = SubscribeFlow::new(config);
                    flow.connect().await?;
                    flow.run_with_shutdown(token).await?;
                }
                _ => anyhow::bail!("Unsupported component: {}", component),
            }
        }
        Commands::Start { component, config_file } => {
            setup_logging(log_level, None)?;
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            if let Some(path_str) = config_file.to_str() { config.load(path_str)?; }
            config.finalize()?;

            let num_instances = config.instances;
            let exe = std::env::current_exe()?;

            for i in 1..=num_instances {
                let mut cmd = Command::new(&exe);
                cmd.arg("run-instance")
                   .arg(&component)
                   .arg(&config_file)
                   .arg(i.to_string());
                
                if cli.debug { cmd.arg("--debug"); }
                if let Some(ref ll) = cli.log_level { cmd.arg("--logLevel").arg(ll); }

                cmd.spawn()?;
                println!("Started instance {} of {}/{} as daemon.", i, component, config.configname.as_deref().unwrap_or("unknown"));
            }
        }
        Commands::Stop { component, config_file } => {
            setup_logging(log_level, None)?;
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            if let Some(path_str) = config_file.to_str() { config.load(path_str)?; }
            config.finalize()?;

            let config_name = config.configname.as_deref();
            for i in 1..=100 { // Check first 100 possible instances
                let pid_file = paths::get_pid_filename(&component, config_name, i);
                if pid_file.exists() {
                    let pid_str = std::fs::read_to_string(&pid_file)?;
                    if let Ok(pid) = pid_str.parse::<i32>() {
                        println!("Stopping instance {} (PID: {})...", i, pid);
                        // Send SIGTERM
                        unsafe { libc::kill(pid, libc::SIGTERM); }
                    }
                    let _ = std::fs::remove_file(pid_file);
                } else if i > config.instances {
                    break;
                }
            }
        }
        Commands::RunInstance { component, config_file, instance } => {
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            if let Some(path_str) = config_file.to_str() { config.load(path_str)?; }
            config.finalize()?;

            let log_file = paths::get_log_filename(&component, config.configname.as_deref(), instance);
            let pid_file = paths::get_pid_filename(&component, config.configname.as_deref(), instance);

            setup_logging(log_level, Some(log_file.clone()))?;
            log::info!("Instance {} starting. Log: {}, PID: {}", instance, log_file.display(), std::process::id());

            if let Some(parent) = pid_file.parent() { std::fs::create_dir_all(parent)?; }
            let mut f = std::fs::File::create(&pid_file)?;
            write!(f, "{}", std::process::id())?;

            let token = tokio_util::sync::CancellationToken::new();
            let token_clone = token.clone();

            // Support SIGTERM for graceful shutdown
            let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = sigterm.recv() => {},
                }
                log::info!("Shutdown signal received, shutting down instance {}...", instance);
                token_clone.cancel();
            });

            let res = match component.as_str() {
                "subscribe" => {
                    let mut flow = SubscribeFlow::new(config);
                    flow.connect().await?;
                    flow.run_with_shutdown(token).await
                }
                _ => anyhow::bail!("Unsupported component: {}", component),
            };

            let _ = std::fs::remove_file(pid_file);
            res?;
        }
    }

    Ok(())
}
