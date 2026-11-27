//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use clap::Parser;
use std::path::PathBuf;
use std::io::Write;
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow, Worklist};
use sr3rs::utils::{setup_logging, resolve_patterns};
use anyhow::Result;

#[derive(Parser)]
#[command(author, version, about = "Post/Announce specific files", long_about = None)]
struct Cli {
    /// Path or pattern to the configuration file(s)
    #[arg(short, long)]
    config: Vec<String>,

    /// Files or paths to announce (via -p or --path)
    #[arg(short = 'p', long = "path", num_args = 1..)]
    paths: Vec<String>,

    /// Positional files to announce
    positional_files: Vec<String>,

    /// Set log level (debug, info, warn, error)
    #[arg(long, alias = "logLevel")]
    log_level: Option<String>,

    /// Enable debug logging (alias for --log_level debug)
    #[arg(long)]
    debug: bool,

    /// Maximum number of messages to process before exiting
    #[arg(long = "messageCountMax", global = true)]
    message_count_max: Option<usize>,

    /// Maximum message rate in messages per second
    #[arg(long = "messageRateMax", global = true, alias = "message_rate_max")]
    message_rate_max: Option<f64>,

    /// Override the default configuration directory
    #[arg(long = "configDir", alias = "config_dir")]
    config_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(config_dir) = cli.config_dir {
        sr3rs::config::paths::set_config_dir_override(config_dir);
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

    setup_logging(log_level, None)?;
    let configs = resolve_patterns(cli.config, "post");
    
    for config_file in configs {
        let comp = sr3rs::utils::detect_component(&config_file);
        let mut config_obj = Config::new();
        config_obj.apply_component_defaults(&comp);
        config_obj.load(&config_file)?;
        if let Some(m) = cli.message_count_max {
            config_obj.message_count_max = m as u32;
        }
        if let Some(r) = cli.message_rate_max {
            config_obj.message_rate_max = r;
        }

        config_obj.finalize()?;
        let mut all_files = cli.paths.clone();
        all_files.extend(cli.positional_files.clone());
        config_obj.post_paths = all_files.iter().map(PathBuf::from).collect();

        if let Err(e) = config_obj.save_state() {
            log::warn!("Failed to save state for {}: {}", config_file, e);
        }

        let mut flow = SubscribeFlow::new(config_obj.clone());
        flow.connect().await?;

        let pid_file = sr3rs::config::paths::get_pid_filename(
            config_obj.host_dir.as_deref(),
            &comp,
            config_obj.configname.as_deref(),
            1,
        );
        if let Some(parent) = pid_file.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(mut f) = std::fs::File::create(&pid_file) {
            let _ = write!(f, "{}", std::process::id());
        }

        let run_res = if config_obj.sleep > 0.0 {
            log::info!("sleep > 0.0 ({}s): running as continuous watcher for {}.", config_obj.sleep, config_file);
            let token = tokio_util::sync::CancellationToken::new();
            let token_clone = token.clone();
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                log::info!("Ctrl+C received, shutting down gracefully...");
                token_clone.cancel();
            });

            flow.run_with_shutdown(token).await
        } else {
            let mut total_count = 0;
            let res = loop {
                let mut worklist = Worklist::new();
                match flow.run_once(&mut worklist).await {
                    Ok(0) => break Ok(()),
                    Ok(batch_count) => total_count += batch_count,
                    Err(e) => break Err(e),
                }
            };
            if res.is_ok() {
                log::info!("Successfully announced {} files using {}.", total_count, config_file);
            }
            let _ = flow.shutdown().await;
            res
        };

        let _ = std::fs::remove_file(&pid_file);
        run_res?;
    }

    Ok(())
}
