//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use clap::{Parser, Subcommand};
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow, Worklist};
use sr3rs::message::Message;
use sr3rs::config::paths;
use sr3rs::utils::{setup_logging, detect_component, resolve_patterns, is_process_running, is_global_config};
use sr3rs::flow::flowcb::gather_file::GatherFilePlugin;
use std::path::Path;
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
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Run a flow in the foreground
    Foreground {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Start flow instances as daemons
    Start {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Stop flow instances
    Stop {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Show the status of flow instances
    Status {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Enable flow instance(s)
    Enable {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Disable flow instance(s)
    Disable {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Stop and cleanup flow instances and broker resources
    Cleanup {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Edit the configuration file(s)
    Edit {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Declare exchanges and queues on the broker
    Declare {
        /// Path or pattern to the configuration file(s)
        config_pattern: Option<String>,
    },
    /// Post/Announce specific files
    Post {
        /// Path or pattern to the configuration file(s)
        #[arg(short, long)]
        config: String,

        /// Files to announce
        files: Vec<String>,
    },
    /// Internal command to run a specific daemon instance
    RunInstance {
        component: String,
        config_file: String,
        instance: u32,
    }
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
        Commands::Show { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);
            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let component = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&component);
                match config.load(&config_file) {
                    Ok(_) => {
                        if let Err(e) = config.finalize() {
                            log::error!("Failed to finalize {}: {}", config_file, e);
                            continue;
                        }
                        let json = serde_json::to_string_pretty(&config)?;
                        println!("--- Configuration: {} ---\n{}\n", config_file, json);
                    }
                    Err(e) => log::error!("Failed to load {}: {}", config_file, e),
                }
            }
        }
        Commands::Foreground { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);
            if configs.len() > 1 {
                anyhow::bail!("Foreground only supports one configuration at a time. Found: {:?}", configs);
            }
            if configs.is_empty() {
                anyhow::bail!("No configuration found matching pattern.");
            }
            
            let config_file = &configs[0];
            let component = detect_component(config_file);
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            config.load(config_file)?;
            config.finalize()?;

            let state_dir = paths::get_user_cache_dir().join(&component).join(config.configname.as_deref().unwrap_or("unknown"));
            if state_dir.join("disabled").exists() {
                anyhow::bail!("Config {} is disabled. It must be enabled before running.", config_file);
            }

            // Save state for foreground run
            config.save_state()?;

            let token = tokio_util::sync::CancellationToken::new();
            let token_clone = token.clone();
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
                log::info!("Ctrl+C received, shutting down gracefully...");
                token_clone.cancel();
            });

            match component.as_str() {
                "subscribe" | "shovel" | "post" | "sarra" | "report" | "sender" | "poll" | "watch" | "winnow" | "cpost" | "cpump" => {
                    let mut flow = SubscribeFlow::new(config);
                    flow.connect().await?;
                    flow.run_with_shutdown(token).await?;
                }
                _ => anyhow::bail!("Unsupported component: {}", component),
            }
        }
        Commands::Start { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);
            let exe = std::env::current_exe()?;

            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let comp = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(e) = config.load(&config_file) {
                    log::error!("Failed to load {}: {}", config_file, e);
                    continue;
                }
                if let Err(e) = config.finalize() {
                    log::error!("Failed to finalize {}: {}", config_file, e);
                    continue;
                }

                let state_dir = paths::get_user_cache_dir().join(&comp).join(config.configname.as_deref().unwrap_or("unknown"));
                if state_dir.join("disabled").exists() {
                    log::error!("Config {} is disabled. It must be enabled before starting.", config_file);
                    continue;
                }

                // Save state for daemon run
                if let Err(e) = config.save_state() {
                    log::error!("Failed to save state for {}: {}", config_file, e);
                    continue;
                }

                let num_instances = config.instances;
                std::fs::create_dir_all(&state_dir)?;
                let state_file = state_dir.join("instances_expected");
                let mut f = std::fs::File::create(state_file)?;
                write!(f, "{}", num_instances)?;

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
        Commands::Stop { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let comp = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(_) = config.load(&config_file) { continue; }
                if let Err(_) = config.finalize() { continue; }

                let config_name = config.configname.as_deref();
                
                let state_dir = paths::get_user_cache_dir().join(&comp).join(config_name.unwrap_or("unknown"));
                let state_file = state_dir.join("instances_expected");
                if state_file.exists() {
                    let _ = std::fs::remove_file(state_file);
                }

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
        Commands::Status { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            if configs.is_empty() {
                println!("No configurations found in {}", paths::get_user_config_dir().display());
                return Ok(());
            }

            println!("{:<35} {:<10} {:<10}", "Component/Config", "State", "Processes");
            println!("{:<35} {:<10} {:<10}", "----------------", "-----", "---------");

            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }

                let comp = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                
                let config_loaded = config.load(&config_file).is_ok();
                let config_finalized = if config_loaded { config.finalize().is_ok() } else { false };

                let config_name = if config_loaded {
                    config.configname.clone()
                } else {
                    std::path::Path::new(&config_file)
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                };

                let mut running_count = 0;
                let expected_count = if config_loaded { config.instances } else { 1 };
                
                let state_dir = paths::get_user_cache_dir().join(&comp).join(config_name.as_deref().unwrap_or("unknown"));
                let state_file = state_dir.join("instances_expected");
                
                let instances_requested = if state_file.exists() {
                    std::fs::read_to_string(state_file).ok().and_then(|s| s.parse().ok()).unwrap_or(0)
                } else {
                    0
                };

                for i in 1..=100 {
                    let pid_file = paths::get_pid_filename(&comp, config_name.as_deref(), i);
                    if pid_file.exists() {
                        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
                            if let Ok(pid) = pid_str.parse::<i32>() {
                                if is_process_running(pid) {
                                    running_count += 1;
                                }
                            }
                        }
                    } else if i > 10 && i > expected_count { 
                        break;
                    }
                }

                let state_exists = if state_dir.exists() {
                    std::fs::read_dir(&state_dir).map(|mut entries| entries.next().is_some()).unwrap_or(false)
                } else {
                    false
                };

                let mut state = if state_dir.join("disabled").exists() {
                    "DISABLED".to_string()
                } else if instances_requested == 0 && running_count == 0 {
                    if state_exists {
                        "STOPPED".to_string()
                    } else {
                        "NEW".to_string()
                    }
                } else if running_count == 0 {
                    "STOPPED".to_string()
                } else if running_count < instances_requested {
                    "PARTIAL".to_string()
                } else {
                    "RUNNING".to_string()
                };

                if !config_loaded || !config_finalized {
                    state = format!("{} (ERR)", state);
                }

                let name = format!("{}/{}", comp, config_name.as_deref().unwrap_or("unknown"));
                println!("{:<35} {:<10} {}/{}", name, state, running_count, expected_count);
            }
            println!();
        }
        Commands::Enable { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let comp = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(e) = config.load(&config_file) {
                    log::error!("Failed to load {}: {}", config_file, e);
                    continue;
                }
                if let Err(e) = config.finalize() {
                    log::error!("Failed to finalize {}: {}", config_file, e);
                    continue;
                }

                let config_name = config.configname.as_deref().unwrap_or("unknown");
                let state_dir = paths::get_user_cache_dir().join(&comp).join(config_name);
                let disabled_file = state_dir.join("disabled");

                if disabled_file.exists() {
                    std::fs::remove_file(disabled_file)?;
                    println!("Enabled {}/{}.", comp, config_name);
                } else {
                    println!("{}/{} is already enabled.", comp, config_name);
                }
            }
        }
        Commands::Disable { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let comp = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(e) = config.load(&config_file) {
                    log::error!("Failed to load {}: {}", config_file, e);
                    continue;
                }
                if let Err(e) = config.finalize() {
                    log::error!("Failed to finalize {}: {}", config_file, e);
                    continue;
                }

                let config_name = config.configname.as_deref().unwrap_or("unknown");
                let state_dir = paths::get_user_cache_dir().join(&comp).join(config_name);
                let disabled_file = state_dir.join("disabled");

                if disabled_file.exists() {
                    println!("{}/{} is already disabled.", comp, config_name);
                    continue;
                }

                // Check if any instances are running
                let mut running = false;
                for i in 1..=config.instances {
                    let pid_file = paths::get_pid_filename(&comp, Some(config_name), i);
                    if pid_file.exists() {
                        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
                            if let Ok(pid) = pid_str.parse::<i32>() {
                                if is_process_running(pid) {
                                    running = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if running {
                    log::error!("Cannot disable {}/{} while it is running!", comp, config_name);
                    continue;
                }

                std::fs::create_dir_all(&state_dir)?;
                std::fs::File::create(disabled_file)?;
                println!("Disabled {}/{}.", comp, config_name);
            }
        }
        Commands::Cleanup { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let comp = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(e) = config.load(&config_file) {
                    log::error!("Failed to load {}: {}", config_file, e);
                    continue;
                }
                if let Err(e) = config.finalize() {
                    log::error!("Failed to finalize {}: {}", config_file, e);
                    continue;
                }

                // First stop any running instances
                let config_name = config.configname.as_deref();
                let state_dir = paths::get_user_cache_dir().join(&comp).join(config_name.unwrap_or("unknown"));
                let state_file = state_dir.join("instances_expected");
                if state_file.exists() {
                    let _ = std::fs::remove_file(state_file);
                }

                let subscriptions_json = state_dir.join("subscriptions.json");
                if subscriptions_json.exists() {
                    let _ = std::fs::remove_file(subscriptions_json);
                }

                let dot_state_json = state_dir.join(".state.json");
                if dot_state_json.exists() {
                    let _ = std::fs::remove_file(dot_state_json);
                }

                let mut stopped_count = 0;
                for i in 1..=100 { 
                    let pid_file = paths::get_pid_filename(&comp, config_name, i);
                    if pid_file.exists() {
                        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
                            if let Ok(pid) = pid_str.parse::<i32>() {
                                if is_process_running(pid) {
                                    unsafe { libc::kill(pid, libc::SIGTERM); }
                                    stopped_count += 1;
                                }
                            }
                        }
                        let _ = std::fs::remove_file(pid_file);
                    } else if i > config.instances {
                        break;
                    }
                }
                if stopped_count > 0 {
                    println!("Stopped {} instance(s) of {}.", stopped_count, config_file);
                }

                // Now cleanup broker resources
                let mut flow = SubscribeFlow::new(config);
                if let Err(e) = flow.connect_full(false, false).await {
                    log::warn!("Failed to connect for cleanup of {}: {}", config_file, e);
                } else {
                    if let Err(e) = flow.cleanup().await {
                        log::error!("Failed to cleanup broker resources for {}: {}", config_file, e);
                    }
                    flow.shutdown().await?;
                }
                println!("Cleanup complete for {}.", config_file);
            }
        }
        Commands::Edit { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);

            if configs.is_empty() {
                println!("No configuration files found to edit.");
                return Ok(());
            }

            let editor = std::env::var("EDITOR").unwrap_or_else(|_| {
                if cfg!(target_os = "windows") {
                    "notepad".to_string()
                } else {
                    "vi".to_string()
                }
            });

            for config_file in configs {
                println!("Editing {} with {}...", config_file, editor);
                let status = Command::new(&editor)
                    .arg(&config_file)
                    .status()?;
                
                if !status.success() {
                    log::error!("Editor exited with non-zero status for {}.", config_file);
                }
            }
        }
        Commands::Declare { config_pattern } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_pattern);
            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let component = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&component);
                if let Err(e) = config.load(&config_file) {
                    log::error!("Failed to load {}: {}", config_file, e);
                    continue;
                }
                if let Err(e) = config.finalize() {
                    log::error!("Failed to finalize {}: {}", config_file, e);
                    continue;
                }

                // Save state for declaration
                if let Err(e) = config.save_state() {
                    log::error!("Failed to save state for {}: {}", config_file, e);
                    continue;
                }

                match component.as_str() {
                    "subscribe" | "shovel" | "post" | "cpost" | "cpump" | "poll" | "report" | "sarra" | "sender" | "watch" | "winnow" => {
                        let mut flow = SubscribeFlow::new(config);
                        if let Err(e) = flow.connect().await {
                            log::error!("Failed to connect for {}: {}", config_file, e);
                            continue;
                        }
                        flow.shutdown().await?;
                    }
                    _ => log::warn!("Declare not implemented for component: {}", component),
                }
                println!("Declaration complete for {}.", config_file);
            }
        }
        Commands::Post { config, files } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(Some(config));
            
            for config_file in configs {
                if is_global_config(&config_file) {
                    continue;
                }
                let comp = detect_component(&config_file);
                let mut config_obj = Config::new();
                config_obj.apply_component_defaults(&comp);
                config_obj.load(&config_file)?;
                config_obj.finalize()?;

                let mut flow = SubscribeFlow::new(config_obj);
                flow.connect().await?;

                let gather_file = GatherFilePlugin::new(flow.config());
                let mut worklist = Worklist::new();
                for file_path_str in &files {
                    let file_path = Path::new(file_path_str);
                    if file_path.is_dir() {
                        log::info!("Scanning directory: {}", file_path_str);
                        let msgs = gather_file.walk(file_path);
                        log::info!("Found {} files in directory: {}", msgs.len(), file_path_str);
                        worklist.ok.extend(msgs);
                    } else {
                        match Message::from_file(file_path, flow.config()) {
                            Ok(msg) => {
                                log::info!("Queuing notification for: {}", file_path_str);
                                worklist.ok.push(msg);
                            }
                            Err(e) => log::error!("Failed to build message for {}: {}", file_path_str, e),
                        }
                    }
                }

                if !worklist.ok.is_empty() {
                    flow.post(&mut worklist).await?;
                    log::info!("Successfully announced {} files using {}.", worklist.ok.len(), config_file);
                }
                flow.shutdown().await?;
            }
        }
        Commands::RunInstance { component, config_file, instance } => {
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            config.load(&config_file)?;
            config.finalize()?;

            let state_dir = paths::get_user_cache_dir().join(&component).join(config.configname.as_deref().unwrap_or("unknown"));
            if state_dir.join("disabled").exists() {
                anyhow::bail!("Config {} is disabled. Instance {} cannot start.", config_file, instance);
            }

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
                "subscribe" | "shovel" | "post" | "sarra" | "report" | "sender" | "poll" | "watch" | "winnow" | "cpost" | "cpump" => {
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
