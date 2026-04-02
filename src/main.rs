use clap::{Parser, Subcommand};
use sr3rs::Config;
use std::path::PathBuf;
use anyhow::Result;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
}

fn main() -> Result<()> {
    env_logger::init();
    let cli = Cli::parse();

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
    }

    Ok(())
}
