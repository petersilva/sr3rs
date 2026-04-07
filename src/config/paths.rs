//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use std::path::PathBuf;
use directories::ProjectDirs;
use lazy_static::lazy_static;

lazy_static! {
    static ref DIRS: Option<ProjectDirs> = ProjectDirs::from("ca.gc.science", "sarracenia", "sr3rs");
}

pub fn get_user_cache_dir() -> PathBuf {
    DIRS.as_ref()
        .map(|d| d.cache_dir().to_path_buf())
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".cache").join("sr3rs")
        })
}

pub fn get_user_config_dir() -> PathBuf {
    DIRS.as_ref()
        .map(|d| d.config_dir().to_path_buf())
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".config").join("sr3rs")
        })
}

pub fn get_log_dir() -> PathBuf {
    get_user_cache_dir().join("log")
}

pub fn get_log_filename(component: &str, configuration: Option<&str>, instance: u32) -> PathBuf {
    let config_part = configuration
        .map(|c| format!("_{}", c.trim_end_matches(".conf")))
        .unwrap_or_default();
    
    get_log_dir().join(format!("{}{}_{:02}.log", component, config_part, instance))
}

pub fn get_pid_filename(component: &str, configuration: Option<&str>, instance: u32) -> PathBuf {
    let config_part = configuration
        .map(|c| c.trim_end_matches(".conf"))
        .unwrap_or("unknown");
    
    get_user_cache_dir()
        .join(component)
        .join(config_part)
        .join(format!("{}_{}_{:02}.pid", component, config_part, instance))
}

pub fn get_metrics_filename(component: &str, configuration: Option<&str>, instance: u32) -> PathBuf {
    let metrics_dir = get_user_cache_dir().join("metrics");
    let config_part = configuration
        .map(|c| format!("_{}", c.trim_end_matches(".conf")))
        .unwrap_or_default();
    
    metrics_dir.join(format!("{}{}_{:02}.json", component, config_part, instance))
}
