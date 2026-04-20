//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use sr3rs::Config;
use sr3rs::flow::{Flow, Worklist, subscribe::SubscribeFlow, sender::SenderFlow};
use sr3rs::config::paths;
use sr3rs::utils::{setup_logging, detect_component, resolve_patterns, is_process_running };
use anyhow::Result;
use std::io::Write;
use std::process::Command;
use axum::{
    routing::{get, post},
    extract::Query,
    Json, Router,
};
use tower_http::cors::CorsLayer;
use sr3rs::ui::{ConfigInfo, NativeBackend, IoBackend};

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

    /// Override the default configuration directory
    #[arg(long = "configDir", global = true, alias = "config_dir")]
    config_dir: Option<PathBuf>,

    /// Confirm you want to do something dangerous (required when operating on multiple configs)
    #[arg(long = "dangerWillRobinson", global = true, default_value_t = 0)]
    danger_will_robinson: usize,
 
    /// Maximum number of messages to process before exiting
    #[arg(long = "messageCountMax", global = true)]
    message_count_max: Option<usize>,

    /// Declare users as well as exchanges and queues
    #[arg(long, global = true)]
    users: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Show the resolved configuration
    Show {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Run a flow in the foreground
    Foreground {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Start flow instances as daemons
    Start {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Stop flow instances
    Stop {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Show the status of flow instances
    Status {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Enable flow instance(s)
    Enable {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Disable flow instance(s)
    Disable {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Stop and cleanup flow instances and broker resources
    Cleanup {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
        #[arg(long = "dangerWillRobinson", default_value_t = 0)]
        danger_will_robinson: usize,
    },
    /// Remove flow configurations and files
    Remove {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
        #[arg(long = "dangerWillRobinson", default_value_t = 0)]
        danger_will_robinson: usize,
    },
    /// Edit the configuration file(s)
    Edit {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Declare exchanges and queues on the broker
    Declare {
        /// Path or pattern to the configuration file(s)
        config_patterns: Vec<String>,
    },
    /// Run native GUI to manage data flow configurations
    View,
    /// A web server to manage data flow configurations with a browser
    #[command(name = "webui")]
    WebUi {
        /// Port to run the web server on
        #[arg(long, default_value_t = 8080)]
        port: u16,
    },
    /// Post/Announce specific files
    Post {
        /// Path or pattern to the configuration file(s)
        #[arg(short, long)]
        config_patterns: Vec<String>,

        /// Files to announce
        files: Vec<String>,
    },
    /// Internal command to run a specific daemon instance
    RunInstance {
        component: String,
        config_file: String,
        instance: u32,
    },
    Sanity {

    }
}

fn get_host_list(cfg: &Config, admins: &std::collections::HashMap<String, sr3rs::broker::Broker>) -> Vec<String> {
    let mut hs = std::collections::HashSet::new();
    if let Some(b) = &cfg.broker { if let Some(h) = b.url.host_str() { hs.insert(h.to_string()); } }
    for sub in &cfg.subscriptions { if let Some(c) = &sub.broker { if let Some(h) = c.url.host_str() { hs.insert(h.to_string()); } } }
    for p in &cfg.publishers { if let Some(c) = &p.broker { if let Some(h) = c.url.host_str() { hs.insert(h.to_string()); } } }
    
    if let Some(b) = &cfg.admin { if let Some(h) = b.url.host_str() { hs.insert(h.to_string()); } }
    if let Some(b) = &cfg.feeder { if let Some(h) = b.url.host_str() { hs.insert(h.to_string()); } }

    if hs.is_empty() {
        admins.keys().cloned().collect()
    } else {
        hs.into_iter().collect()
    }
}

async fn declare_rabbitmq_user(admin_broker: &sr3rs::broker::Broker, username: &str, password: &str, role: &str) -> Result<()> {
    let client = reqwest::Client::new();
    let hostname = admin_broker.url.host_str().unwrap_or("localhost");
    let port = if admin_broker.url.scheme() == "amqps" { 15671 } else { 15672 };
    let scheme = if admin_broker.url.scheme() == "amqps" { "https" } else { "http" };
    
    let base_url = format!("{}://{}:{}", scheme, hostname, port);
    let admin_user = admin_broker.url.username();
    let admin_pass = admin_broker.url.password().unwrap_or("");

    log::info!("Declaring user {} with role {} on broker {}", username, role, admin_broker.redacted());

    // 1. Put user
    let user_url = format!("{}/api/users/{}", base_url, username);
    let tags = if role == "admin" { "administrator" } else { "" };
    let user_body = serde_json::json!({
        "password": password,
        "tags": tags
    });

    let res = client.put(&user_url)
        .basic_auth(admin_user, Some(admin_pass))
        .json(&user_body)
        .send()
        .await?;

    if !res.status().is_success() {
        let err_text = res.text().await?;
        anyhow::bail!("Failed to declare user {}: {} {}", username, user_url, err_text);
    }

    // 2. Put permissions
    let (c, w, r) = match role {
        "admin" | "feeder" | "manager" => (".*".to_string(), ".*".to_string(), ".*".to_string()),
        "source" => (
            format!("^q_{}.*|^xs_{}.*", username, username),
            format!("^q_{}.*|^xs_{}.*", username, username),
            format!("^q_{}.*|^x[lrs]_{}.*|^x.*public$", username, username)
        ),
        "subscribe" | "subscriber" => (
            format!("^q_{}.*", username),
            format!("^q_{}.*|^xs_{}$", username, username),
            format!("^q_{}.*|^x[lrs]_{}.*|^x.*public$", username, username)
        ),
        _ => (".*".to_string(), ".*".to_string(), ".*".to_string()),
    };

    let vhost = "/"; // SR3 defaults to / for global user declarations
    let permissions_url = format!("{}/api/permissions/{}/{}", base_url, urlencoding::encode(vhost), username);
    let permissions_body = serde_json::json!({
        "configure": c,
        "write": w,
        "read": r
    });

    let res = client.put(&permissions_url)
        .basic_auth(admin_user, Some(admin_pass))
        .json(&permissions_body)
        .send()
        .await?;

    if !res.status().is_success() {
        let err_text = res.text().await?;
        anyhow::bail!("Failed to declare permissions for {}: {}", username, err_text);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(ref config_dir) = cli.config_dir {
        paths::set_config_dir_override(config_dir.clone());
    }

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
        Commands::Show { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns, "subscribe");
            for config_file in configs {
                log::debug!("Resolved config: {}", config_file);
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
        Commands::Foreground { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns, "subscribe");
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
            if let Some(m) = cli.message_count_max {
                config.message_count_max = m as u32;
            }

            config.finalize()?;

            let state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&component).join(config.configname.as_deref().unwrap_or("unknown"));
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
                "sender" => {
                    let mut flow = SenderFlow::new(config);
                    flow.connect_full(true, true).await?;
                    flow.run_with_shutdown(token).await?;
                }
                "subscribe" | "shovel" | "post" | "sarra" | "report" | "poll" | "watch" | "winnow" | "cpost" | "cpump" => {
                    let mut flow = SubscribeFlow::new(config);
                    flow.connect().await?;
                    flow.run_with_shutdown(token).await?;
                }
                _ => anyhow::bail!("Unsupported component: {}", component),
            }
        }
        Commands::Start { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");
            let exe = std::env::current_exe()?;

            for config_file in configs {
                
                let comp = detect_component(&config_file);
                // Skip interactive components for start command
                if comp == "post" || comp == "cpost" {
                    log::info!("Skipping start for interactive component: {} ({})", comp, config_file);
                    continue;
                }

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

                let state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&comp).join(config.configname.as_deref().unwrap_or("unknown"));
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
                    let pid_file = paths::get_pid_filename(config.host_dir.as_deref(), &comp, config.configname.as_deref(), i);
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
                    if let Some(ref cd) = cli.config_dir { cmd.arg("--configDir").arg(cd); }

                    cmd.stdout(std::process::Stdio::null());
                    cmd.stderr(std::process::Stdio::null());
                    cmd.stdin(std::process::Stdio::null());

                    cmd.spawn()?;
                }
                println!("Started {} instance(s) of {} as daemons.", num_instances, config_file);
            }
        }
        Commands::Stop { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");

            for config_file in configs {
                let comp = detect_component(&config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&comp);
                if let Err(_) = config.load(&config_file) { continue; }
                if let Err(_) = config.finalize() { continue; }

                let config_name = config.configname.as_deref();
                
                let state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&comp).join(config_name.unwrap_or("unknown"));
                let state_file = state_dir.join("instances_expected");
                if state_file.exists() {
                    let _ = std::fs::remove_file(state_file);
                }

                let mut stopped_count = 0;
                for i in 1..=100 { 
                    let pid_file = paths::get_pid_filename(config.host_dir.as_deref(), &comp, config_name, i);
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
        Commands::Status { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");

            if configs.is_empty() {
                println!("No configurations found in {}", paths::get_user_config_dir().display());
                return Ok(());
            }

            println!("{:<35} {:<10} {:<10} {:<10} {:<10}", "Component/Config", "State", "Processes", "Msg/s", "Last HK");
            println!("{:<35} {:<10} {:<10} {:<10} {:<10}", "----------------", "-----", "---------", "-----", "-------");

            for config_file in configs {

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
                let mut crashed_count = 0;
                let expected_count = if config_loaded { config.instances } else { 1 };

                let mut state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&comp).join(config_name.as_deref().unwrap_or("unknown"));
                
                if !config_loaded && !state_dir.exists() {
                    if let Ok(h) = hostname::get() {
                        let h_str = h.to_string_lossy().to_string();
                        let guessed_dir = paths::get_user_cache_dir(Some(&h_str)).join(&comp).join(config_name.as_deref().unwrap_or("unknown"));
                        if guessed_dir.exists() {
                            state_dir = guessed_dir;
                            config.host_dir = Some(h_str);
                        }
                    }
                }

                let instances_requested_file = state_dir.join("instances_expected");

                let instances_requested = if instances_requested_file.exists() {
                    std::fs::read_to_string(instances_requested_file).ok().and_then(|s| s.parse().ok()).unwrap_or(0)
                } else {
                    0
                };

                for i in 1..=100 {
                    let pid_file = paths::get_pid_filename(config.host_dir.as_deref(), &comp, config_name.as_deref(), i);
                    if pid_file.exists() {
                        if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
                            if let Ok(pid) = pid_str.trim().parse::<i32>() {
                                if is_process_running(pid) {
                                    running_count += 1;
                                } else {
                                    crashed_count += 1;
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
                    "stop".to_string()
                } else if crashed_count > 0 {
                    "crashed".to_string()
                } else if instances_requested == 0 && running_count == 0 {
                    if comp == "post" || comp == "cpost" {
                        "inte".to_string()
                    } else if state_exists {
                        "stop".to_string()
                    } else {
                        "new".to_string()
                    }
                } else if running_count == 0 {
                    if comp == "post" || comp == "cpost" {
                        "inte".to_string()
                    } else {
                        "stop".to_string()
                    }
                } else if running_count < instances_requested {
                    "part".to_string()
                } else {
                    "run".to_string()
                };

                if running_count > 0 && !config.vip.is_empty() && !config.has_vip() {
                    state = "wvip".to_string();
                }

                if !config_loaded || !config_finalized {
                    state = format!("{} (ERR)", state);
                }

                // Try to read metrics
                let mut msg_rate = "-".to_string();
                let mut last_hk = "-".to_string();

                if let Some(ref c_name) = config_name {
                    for i in 1..=100 {
                        let metrics_file = paths::get_metrics_filename(config.host_dir.as_deref(), &comp, Some(c_name), i);
                        if metrics_file.exists() {
                            if let Ok(content) = std::fs::read_to_string(metrics_file) {
                                if let Ok(metrics) = serde_json::from_str::<serde_json::Value>(&content) {
                                    if let Some(flow) = metrics.get("flow") {
                                        if let (Some(msg_out), Some(start_time_str)) = (flow.get("msg_count_out"), flow.get("start_time")) {
                                            let msg_count = msg_out.as_u64().unwrap_or(0);
                                            if let Ok(start_time) = chrono::DateTime::parse_from_rfc3339(start_time_str.as_str().unwrap_or("")) {
                                                let duration = chrono::Utc::now().signed_duration_since(start_time);
                                                let secs = duration.num_seconds().max(1);
                                                msg_rate = format!("{:.2}", msg_count as f64 / secs as f64);
                                            }
                                        }
                                        if let Some(last_hk_str) = flow.get("last_housekeeping") {
                                             if let Ok(last_hk_dt) = chrono::DateTime::parse_from_rfc3339(last_hk_str.as_str().unwrap_or("")) {
                                                 let duration = chrono::Utc::now().signed_duration_since(last_hk_dt);
                                                 let secs = duration.num_seconds();
                                                 if secs < 60 {
                                                     last_hk = format!("{}s", secs);
                                                 } else if secs < 3600 {
                                                     last_hk = format!("{}m", secs / 60);
                                                 } else {
                                                     last_hk = format!("{}h", secs / 3600);
                                                 }
                                             }
                                        }
                                    }
                                }
                            }
                            break; 
                        } else if i > expected_count && i > 10 {
                            break;
                        }
                    }
                }

                let name = format!("{}/{}", comp, config_name.as_deref().unwrap_or("unknown"));
                println!("{:<35} {:<10} {:<10} {:<10} {:<10}", name, state, format!("{}/{}", running_count, expected_count), msg_rate, last_hk);
            }

            println!();
        }
        Commands::Enable { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");

            for config_file in configs {
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
                let state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&comp).join(config_name);
                let disabled_file = state_dir.join("disabled");

                if disabled_file.exists() {
                    std::fs::remove_file(disabled_file)?;
                    println!("Enabled {}/{}.", comp, config_name);
                } else {
                    println!("{}/{} is already enabled.", comp, config_name);
                }
            }
        }
        Commands::Disable { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");

            for config_file in configs {
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
                let state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&comp).join(config_name);
                let disabled_file = state_dir.join("disabled");

                if disabled_file.exists() {
                    println!("{}/{} is already disabled.", comp, config_name);
                    continue;
                }

                // Check if any instances are running
                let mut running = false;
                for i in 1..=config.instances {
                    let pid_file = paths::get_pid_filename(config.host_dir.as_deref(), &comp, Some(config_name), i);
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
        Commands::Cleanup { config_patterns, danger_will_robinson } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");

            if configs.len() > 1 && danger_will_robinson != configs.len() {
                log::error!("specify --dangerWillRobinson={} to cleanup multiple configs when > 1 involved. (actual: {}, given: {})", 
                    configs.len(), configs.len(), danger_will_robinson);
                return Ok(());
            }

            for config_file in configs {
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
                let state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&comp).join(config_name.unwrap_or("unknown"));
                let state_file = state_dir.join("instances_expected");
                if state_file.exists() {
                    let _ = std::fs::remove_file(state_file);
                }

                let subscriptions_json = state_dir.join("subscriptions.json");
                if subscriptions_json.exists() {
                    if let Err(e) = std::fs::remove_file(&subscriptions_json) {
                        log::error!("Failed to remove {}: {}", subscriptions_json.display(), e);
                    } else {
                        log::info!("Removed {}", subscriptions_json.display());
                    }
                }

                let dot_state_json = state_dir.join(".state.json");
                if dot_state_json.exists() {
                    if let Err(e) = std::fs::remove_file(&dot_state_json) {
                        log::error!("Failed to remove {}: {}", dot_state_json.display(), e);
                    } else {
                        log::info!("Removed {}", dot_state_json.display());
                    }
                }

                let mut stopped_count = 0;
                for i in 1..=100 { 
                    let pid_file = paths::get_pid_filename(config.host_dir.as_deref(), &comp, config_name, i);
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
                match comp.as_str() {
                    "sender" => {
                        let mut flow = SenderFlow::new(config);
                        if let Err(e) = flow.connect_full(false, false).await {
                            log::warn!("Failed to connect for cleanup of {}: {}", config_file, e);
                        } else {
                            if let Err(e) = flow.cleanup().await {
                                log::error!("Failed to cleanup broker resources for {}: {}", config_file, e);
                            }
                            flow.shutdown().await?;
                        }
                    }
                    _ => {
                        let mut flow = SubscribeFlow::new(config);
                        if let Err(e) = flow.connect_full(false, false).await {
                            log::warn!("Failed to connect for cleanup of {}: {}", config_file, e);
                        } else {
                            if let Err(e) = flow.cleanup().await {
                                log::error!("Failed to cleanup broker resources for {}: {}", config_file, e);
                            }
                            flow.shutdown().await?;
                        }
                    }
                }
            }
        }
        Commands::Edit { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");

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
        Commands::Declare { config_patterns } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns.clone(),"subscribe");

            let mut admins = std::collections::HashMap::new();
            let mut unique_users = std::collections::HashSet::new();
            let mut unique_exchanges = std::collections::HashSet::new();

            // Load all credentials once
            let mut cred_db = sr3rs::config::credentials::CredentialDb::new();
            let cred_file = paths::get_user_config_dir().join("credentials.conf");
            if let Err(e) = cred_db.load(&cred_file) {
                log::warn!("Failed to load credentials from {}: {}", cred_file.display(), e);
            }

            // Phase 1: Collect all admin brokers and feeders from ALL configs to build the admins map
            log::debug!("Phase 1: Collecting admin brokers...");
            for config_file in &configs {
                let mut cfg = Config::new();
                let comp = detect_component(config_file);
                cfg.apply_component_defaults(&comp);
                if cfg.load(config_file).is_ok() && cfg.finalize().is_ok() {
                    if let Some(ref b) = cfg.admin {
                        let host = b.url.host_str().unwrap_or("localhost").to_string();
                        // Admin takes precedence over feeder
                        admins.insert(host, b.clone());
                    }
                    if let Some(ref b) = cfg.feeder {
                        let host = b.url.host_str().unwrap_or("localhost").to_string();
                        if !admins.contains_key(&host) {
                            admins.insert(host, b.clone());
                        }
                    }
                }
            }

            // Phase 2: Resolve admin passwords
            log::debug!("Phase 2: Resolving admin passwords...");
            for admin in admins.values_mut() {
                if admin.url.password().is_none() {
                    if let Some(cred) = cred_db.get(&admin.url.to_string()) {
                        if let Some(pw) = cred.url.password() {
                            let _ = admin.url.set_password(Some(pw));
                        }
                    }
                }
            }

            // Phase 3: Collect unique users and admin exchanges across all configs
            log::debug!("Phase 3: Collecting unique users and admin exchanges...");
            for config_file in &configs {
                let mut cfg = Config::new();
                let comp = detect_component(config_file);
                cfg.apply_component_defaults(&comp);
                if cfg.load(config_file).is_ok() && cfg.finalize().is_ok() {
                    // Collect users
                    for (username, role) in &cfg.declared_users {
                        let host_list = get_host_list(&cfg, &admins);
                        for host in host_list {
                            unique_users.insert((username.clone(), host, role.clone()));
                        }
                    }

                    // Collect admin exchanges
                    for exchange in &cfg.declared_exchanges {
                        if let Some(admin_broker) = &cfg.admin {
                            let host = admin_broker.url.host_str().unwrap_or("localhost").to_string();
                            unique_exchanges.insert((exchange.clone(), host));
                        } else if let Some(feeder_broker) = &cfg.feeder {
                            let host = feeder_broker.url.host_str().unwrap_or("localhost").to_string();
                            unique_exchanges.insert((exchange.clone(), host));
                        }
                    }
                }
            }

            // Phase 4: Declare Users if requested
            if cli.users {
                log::info!("Phase 4: Declaring users...");
                for (username, host, role) in &unique_users {
                    if let Some(admin_broker) = admins.get(host) {
                        let dummy_url = format!("amqp://{}@{}/", username, host);
                        if let Some(user_cred) = cred_db.get(&dummy_url) {
                            let password = user_cred.url.password().unwrap_or("");
                            if let Err(e) = declare_rabbitmq_user(admin_broker, username, password, role).await {
                                log::error!("Failed to declare user {}: {}", username, e);
                            }
                        } else {
                            log::warn!("No password found for user {} on {}, skipping user declaration.", username, host);
                        }
                    } else {
                        log::warn!("No admin broker found for host {}, cannot declare user {}.", host, username);
                    }
                }
            }

            // Phase 5: Declare Admin Exchanges
            log::info!("Phase 5: Declaring admin exchanges...");
            for (exchange, host) in &unique_exchanges {
                 if let Some(broker) = admins.get(host) {
                     log::info!("Declaring admin exchange {} on {}", exchange, host);
                     if let Ok(mut moth) = sr3rs::moth::MothFactory::new(broker, false).await {
                         if let Err(e) = moth.declare_exchange(exchange, "topic").await {
                             log::error!("Failed to declare admin exchange {}: {}", exchange, e);
                         }
                         let _ = moth.close().await;
                     }
                 }
            }

            // Phase 6: Declare flow exchanges for all configs
            log::info!("Phase 6: Declaring flow exchanges...");
            let mut flows: Vec<(String, String, Box<dyn Flow>)> = Vec::new();

            for config_file in &configs {
                let component = detect_component(config_file);
                let mut config = Config::new();
                config.apply_component_defaults(&component);
                if let Err(_) = config.load(config_file) { continue; }
                if let Err(_) = config.finalize() { continue; }

                let mut flow: Box<dyn Flow> = match component.as_str() {
                    "sender" => Box::new(SenderFlow::new(config)),
                    _ => Box::new(SubscribeFlow::new(config)),
                };

                if let Err(e) = flow.connect_exchanges().await {
                    log::error!("Failed to declare exchanges for {}: {}", config_file, e);
                }
                flows.push((config_file.clone(), component, flow));
            }

            // Phase 7: Declare queues and bindings for all configs
            log::info!("Phase 7: Declaring queues and bindings...");
            for (config_file, _component, mut flow) in flows {
                if let Err(e) = flow.connect_queues().await {
                    log::error!("Failed to declare queues/bindings for {}: {}", config_file, e);
                } else {
                    println!("Declaration complete for {}.", config_file);
                    if let Err(e) = flow.config().save_state() {
                        log::error!("Failed to save state for {}: {}", config_file, e);
                    }
                }
                let _ = flow.shutdown().await;
            }
        }
        Commands::View => {
            // Find the sr3rs_ui binary. It should be in the same directory as the current executable.
            let current_exe = std::env::current_exe()?;
            let ui_exe = current_exe.with_file_name("sr3rs_ui");
            
            if !ui_exe.exists() {
                anyhow::bail!("sr3rs_ui binary not found at {}. Please build it with 'cargo build --bin sr3rs_ui'", ui_exe.display());
            }

            log::info!("Starting UI: {}", ui_exe.display());
            let mut cmd = Command::new(ui_exe);
            if let Some(ref cd) = cli.config_dir {
                cmd.arg("--configDir").arg(cd);
            }
            cmd.spawn()?
                .wait()?;
        }
        Commands::WebUi { port } => {
            setup_logging(log_level, None)?;
            log::info!("Starting Web UI server on port {}", port);
            log::info!("Open http://localhost:{} in your browser", port);

            // Serve the 'dist' directory for static files (Wasm, HTML, JS)
            let serve_dir = tower_http::services::ServeDir::new("dist")
            .fallback(tower_http::services::ServeFile::new("dist/index.html"));

            let app = Router::new()
            .route("/api/configs", get(list_configs))
            .route("/api/read", get(read_file))
            .route("/api/write", post(write_file))
            .route("/api/positions", get(get_positions).post(post_positions))
            .route("/api/action", post(handle_action))
            .fallback_service(serve_dir)
            .layer(CorsLayer::permissive());

            let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
            axum::serve(listener, app).await?;
            }
            Commands::Post { config_patterns, files } => {            setup_logging(log_level, None)?;

            log::warn!("config: {:?}  files: {:?} ", config_patterns, files);

            let comp = "post".to_string();
            let configs = resolve_patterns(config_patterns, &comp);
            

            for config_file in configs {
                log::warn!("in the config loop, config_file: {:?}", config_file);
                let mut config_obj = Config::new();
                config_obj.apply_component_defaults(&comp);
                config_obj.load(&config_file)?;
                config_obj.finalize()?;

                // Pass the files from CLI to the gather plugin via config
                config_obj.post_paths = files.iter().map(PathBuf::from).collect();

                let mut flow = SubscribeFlow::new(config_obj);
                let mut total_announced = 0;
                let mut total_found = 0;

                flow.connect().await?;

                loop {
                    let mut worklist = Worklist::new();
                    let batch_count = flow.run_once(&mut worklist).await?;
                    total_found += batch_count; // Actually, gather gives us count in incoming, run_once returns total processed
                    if batch_count == 0 {
                         break;
                    }
                    // Let's assume the gather puts it in worklist.incoming, then run_once moves it to worklist.ok
                    // Actually run_once is returning `processed`
                    total_announced += batch_count;
                }
                flow.shutdown().await?;
                log::info!("Successfully announced {} files (out of {} found) using {}.", total_announced, total_found, config_file);
            }
        }
        Commands::Remove { config_patterns, danger_will_robinson } => {
            setup_logging(log_level, None)?;
            let configs = resolve_patterns(config_patterns,"subscribe");

            if configs.len() > 1 && danger_will_robinson != configs.len() {
                log::error!("specify --dangerWillRobinson={} to remove multiple configs when > 1 involved. (actual: {}, given: {})", 
                    configs.len(), configs.len(), danger_will_robinson);
                return Ok(());
            }

            for config_file in configs {
                
                log::info!("Removing configuration: {}", config_file);
                if std::path::Path::new(&config_file).exists() {
                    let _ = std::fs::remove_file(&config_file);
                }
                // Further cleanup (like state dir removal) could happen here or share logic with Cleanup
            }
        }
        Commands::RunInstance { component, config_file, instance } => {
            let mut config = Config::new();
            config.apply_component_defaults(&component);
            config.load(&config_file)?;
            config.finalize()?;

            let state_dir = paths::get_user_cache_dir(config.host_dir.as_deref()).join(&component).join(config.configname.as_deref().unwrap_or("unknown"));
            if state_dir.join("disabled").exists() {
                anyhow::bail!("Config {} is disabled. Instance {} cannot start.", config_file, instance);
            }

            let log_file = paths::get_log_filename(config.host_dir.as_deref(), &component, config.configname.as_deref(), instance);
            let pid_file = paths::get_pid_filename(config.host_dir.as_deref(), &component, config.configname.as_deref(), instance);

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
                "sender" => {
                    let mut flow = SenderFlow::new(config);
                    flow.connect_full(true, true).await?;
                    flow.run_with_shutdown(token).await
                }
                "subscribe" | "shovel" | "post" | "sarra" | "report" | "poll" | "watch" | "winnow" | "cpost" | "cpump" => {
                    let mut flow = SubscribeFlow::new(config);
                    flow.connect().await?;
                    flow.run_with_shutdown(token).await
                }
                _ => anyhow::bail!("Unsupported component: {}", component),
            };

            let _ = std::fs::remove_file(pid_file);
            res?;
        }
       Commands::Sanity { } => {
            log::warn!("Sanity not implemented");
        }
    }

    Ok(())
}

#[derive(serde::Deserialize)]
struct FilePath {
    path: String,
}

async fn list_configs() -> Json<Vec<ConfigInfo>> {
    let backend = NativeBackend;
    Json(backend.load_configs().await)
}

async fn read_file(Query(params): Query<FilePath>) -> Result<String, (axum::http::StatusCode, String)> {
    let backend = NativeBackend;
    backend.read_file(&params.path).await.map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))
}

async fn write_file(Query(params): Query<FilePath>, body: String) -> Result<(), (axum::http::StatusCode, String)> {
    let backend = NativeBackend;
    backend.write_file(&params.path, &body).await.map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))
}

async fn get_positions() -> Json<std::collections::HashMap<String, (f32, f32)>> {
    let backend = NativeBackend;
    Json(backend.load_positions().await)
}

async fn post_positions(axum::extract::Json(positions): axum::extract::Json<std::collections::HashMap<String, (f32, f32)>>) -> Result<(), (axum::http::StatusCode, String)> {
    let backend = NativeBackend;
    backend.save_positions(positions).await.map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e))
}

#[derive(serde::Deserialize)]
struct ActionPayload {
    action: String,
    target: String,
}

async fn handle_action(axum::extract::Json(payload): axum::extract::Json<ActionPayload>) -> Result<String, (axum::http::StatusCode, String)> {
    let backend = NativeBackend;
    match backend.execute_action(&payload.action, &payload.target).await {
        Ok(output) => Ok(output),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e)),
    }
}
