//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use clap::Parser;
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow, Worklist};
use sr3rs::message::Message;
use sr3rs::utils::{setup_logging, detect_component, resolve_patterns};
use std::path::Path;
use anyhow::Result;

#[derive(Parser)]
#[command(author, version, about = "Post/Announce specific files", long_about = None)]
struct Cli {
    /// Path or pattern to the configuration file(s)
    #[arg(short, long)]
    config: String,

    /// Files to announce
    files: Vec<String>,

    /// Set log level (debug, info, warn, error)
    #[arg(long, alias = "logLevel")]
    log_level: Option<String>,

    /// Enable debug logging (alias for --log_level debug)
    #[arg(long)]
    debug: bool,
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

    setup_logging(log_level, None)?;
    let configs = resolve_patterns(Some(cli.config));
    
    for config_file in configs {
        let comp = detect_component(&config_file);
        let mut config_obj = Config::new();
        config_obj.apply_component_defaults(&comp);
        config_obj.load(&config_file)?;
        config_obj.finalize()?;

        let mut flow = SubscribeFlow::new(config_obj);
        flow.connect().await?;

        let mut worklist = Worklist::new();
        for file_path in &cli.files {
            match Message::from_file(Path::new(file_path), flow.config()) {
                Ok(msg) => {
                    log::info!("Queuing notification for: {}", file_path);
                    worklist.ok.push(msg);
                }
                Err(e) => log::error!("Failed to build message for {}: {}", file_path, e),
            }
        }

        if !worklist.ok.is_empty() {
            flow.post(&mut worklist).await?;
            log::info!("Successfully announced {} files using {}.", worklist.ok.len(), config_file);
        }
        flow.shutdown().await?;
    }

    Ok(())
}
