use clap::{Parser, Subcommand};
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow};
use sr3rs::config::paths;
use std::path::PathBuf;
use anyhow::Result;
use std::io::Write;
use std::process::Command;
use glob::glob;

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
        #[arg(short, long)]
        component: Option<String>,

        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Run a flow in the foreground
    Foreground {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long)]
        component: Option<String>,

        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Start flow instances as daemons
    Start {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long)]
        component: Option<String>,

        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Stop flow instances
    Stop {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long)]
        component: Option<String>,

        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Show the status of flow instances
    Status {
        /// Component name (e.g., subscribe, poll, post)
        #[arg(short, long)]
        component: Option<String>,

        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Internal command to run a specific daemon instance
    RunInstance {
        component: String,
        config_file: String,
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

fn detect_component(component: Option<String>, config_path: &str) -> String {
    if let Some(c) = component {
        return c;
    }

    let parts: Vec<&str> = config_path.split('/').collect();
    if parts.len() > 1 {
        let first = parts[0];
        match first {
            "subscribe" | "poll" | "post" | "watch" | "winnow" | "shovel" | "sender" => {
                return first.to_string();
            }
            _ => {}
        }
    }

    "subscribe".to_string()
}

fn is_process_running(pid: i32) -> bool {
    if pid <= 0 { return false; }
    unsafe { libc::kill(pid, 0) == 0 }
}

fn resolve_patterns(pattern: Option<String>) -> Vec<String> {
    let config_dir = paths::get_user_config_dir();
    let pattern = pattern.unwrap_or_else(|| "*/*.conf".to_string());
    
    let mut results = Vec::new();
    
    if std::path::Path::new(&pattern).exists() && std::path::Path::new(&pattern).is_file() {
        results.push(pattern.clone());
        return results;
    }

    let full_pattern = if pattern.contains('*') {
        if pattern.contains('/') {
            config_dir.join(&pattern).to_string_lossy().to_string()
        } else {
            config_dir.join("*").join(&pattern).to_string_lossy().to_string()
        }
    } else {
        config_dir.join("*").join(format!("{}*", pattern)).to_string_lossy().to_string()
    };

    if let Ok(entries) = glob(&full_pattern) {
        for entry in entries.flatten() {
            if entry.is_file() {
                results.push(entry.to_string_lossy().to_string());
            }
        }
    }

    if results.is_empty() && !pattern.ends_with(".conf") {
        let conf_pattern = if pattern.contains('/') {
            config_dir.join(format!("{}.conf", pattern)).to_string_lossy().to_string()
        } else {
            config_dir.join("*").join(format!("{}.conf", pattern)).to_string_lossy().to_string()
        };
        if let Ok(entries) = glob(&conf_pattern) {
            for entry in entries.flatten() {
                if entry.is_file() {
                    results.push(entry.to_string_lossy().to_string());
                }
            }
        }
    }

    results
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
        Commands::Show { component, config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);
            for config_file in configs {
                let component = detect_component(component.clone(), &config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&component);
                config.load(&config_file)?;
                config.finalize()?;
                let json = serde_json::to_string_pretty(&config)?;
                println!("--- Configuration: {} ---\n{}\n", config_file, json);
            }
        }
        Commands::Foreground { component, config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);
            if configs.len() > 1 {
                anyhow::bail!("Foreground only supports one configuration at a time. Found: {:?}", configs);
            }
            if configs.is_empty() {
                anyhow::bail!("No configuration found matching pattern.");
            }
            
            let config_file = &configs[0];
            let component = detect_component(component, config_file);
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            config.load(config_file)?;
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
        Commands::Start { component, config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);
            let exe = std::env::current_exe()?;

            for config_file in configs {
                let comp = detect_component(component.clone(), &config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                config.load(&config_file)?;
                config.finalize()?;

                let num_instances = config.instances;
                for i in 1..=num_instances {
                    let pid_file = paths::get_pid_filename(&comp, config.configname.as_deref(), i);
                    if pid_file.exists() {
                        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
                            if let Ok(pid) = pid_str.parse::<i32>() {
                                if is_process_running(pid) {
                                    continue;
                                }
                            }
                        }
                    }

                    let mut cmd = Command::new(&exe);
                    cmd.arg("run-instance")
                       .arg(&comp)
                       .arg(&config_file)
                       .arg(i.to_string());
                    
                    if cli.debug { cmd.arg("--debug"); }
                    if let Some(ref ll) = cli.log_level { cmd.arg("--logLevel").arg(ll); }

                    cmd.spawn()?;
                }
                println!("Started {} instance(s) of {} as daemons.", num_instances, config_file);
            }
        }
        Commands::Stop { component, config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            for config_file in configs {
                let comp = detect_component(component.clone(), &config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(_) = config.load(&config_file) { continue; }
                config.finalize()?;

                let config_name = config.configname.as_deref();
                let mut stopped_count = 0;
                for i in 1..=100 { 
                    let pid_file = paths::get_pid_filename(&comp, config_name, i);
                    if pid_file.exists() {
                        let pid_str = std::fs::read_to_string(&pid_file)?;
                        if let Ok(pid) = pid_str.parse::<i32>() {
                            if is_process_running(pid) {
                                unsafe { libc::kill(pid, libc::SIGTERM); }
                                stopped_count += 1;
                            }
                        }
                        let _ = std::fs::remove_file(pid_file);
                    } else if i > config.instances {
                        break;
                    }
                }
                if stopped_count > 0 {
                    println!("Stopped {} instance(s) of {}.", stopped_count, config_file);
                } else {
                    println!("No running instances found for {}.", config_file);
                }
            }
        }
        Commands::Status { component, config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            println!("{:<25} {:<10} {:<10}", "Component/Config", "State", "Processes");
            println!("{:<25} {:<10} {:<10}", "----------------", "-----", "---------");

            for config_file in configs {
                let comp = detect_component(component.clone(), &config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(_) = config.load(&config_file) { continue; }
                config.finalize()?;

                let config_name = config.configname.as_deref();
                let mut running_count = 0;
                let expected_count = config.instances;

                for i in 1..=100 {
                    let pid_file = paths::get_pid_filename(&comp, config_name, i);
                    if pid_file.exists() {
                        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
                            if let Ok(pid) = pid_str.parse::<i32>() {
                                if is_process_running(pid) {
                                    running_count += 1;
                                }
                            }
                        }
                    } else if i > expected_count {
                        break;
                    }
                }

                let state = if running_count == 0 {
                    "STOPPED"
                } else if running_count < expected_count {
                    "PARTIAL"
                } else {
                    "RUNNING"
                };

                let name = format!("{}/{}", comp, config.configname.as_deref().unwrap_or("unknown"));
                println!("{:<25} {:<10} {}/{}", name, state, running_count, expected_count);
            }
            println!();
        }
        Commands::RunInstance { component, config_file, instance } => {
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            config.load(&config_file)?;
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
