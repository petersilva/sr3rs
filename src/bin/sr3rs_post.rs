//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use clap::Parser;
use sr3rs::Config;
use sr3rs::flow::{Flow, subscribe::SubscribeFlow, Worklist};
use sr3rs::message::Message;
use sr3rs::utils::{setup_logging, resolve_patterns};
use std::path::Path;
use anyhow::Result;

#[derive(Parser)]
#[command(author, version, about = "Post/Announce specific files", long_about = None)]
struct Cli {
    /// Path or pattern to the configuration file(s)
    #[arg(short, long)]
    config: Vec<String>,

    /// Files to announce
    files: Vec<String>,

    /// Set log level (debug, info, warn, error)
    #[arg(long, alias = "logLevel")]
    log_level: Option<String>,

    /// Enable debug logging (alias for --log_level debug)
    #[arg(long)]
    debug: bool,

    /// Maximum number of messages to process before exiting
    #[arg(long = "messageCountMax", global = true)]
    message_count_max: Option<usize>,

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
    let comp = "post".to_string();
    let configs = resolve_patterns(cli.config, &comp);
    
    for config_file in configs {
        let mut config_obj = Config::new();
        config_obj.apply_component_defaults(&comp);
        config_obj.load(&config_file)?;
         if let Some(m) = cli.message_count_max {
                config_obj.message_count_max = m as u32;
         }

        config_obj.finalize()?;
        config_obj.post_paths = cli.files.clone();

        let mut flow = SubscribeFlow::new(config_obj);
        flow.connect().await?;

        let mut worklist = Worklist::new();

        let count = flow.run_once(&mut worklist).await?;

        //for file_path in &cli.files {

        //}

        if !worklist.ok.is_empty() {
            flow.post(&mut worklist).await?;
            log::info!("Successfully announced {} files using {}.", worklist.ok.len(), config_file);
        }
        flow.shutdown().await?;
    }

    Ok(())
}
