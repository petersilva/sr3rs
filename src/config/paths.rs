//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use std::path::PathBuf;
use directories::ProjectDirs;
use std::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
    static ref CONFIG_DIR_OVERRIDE: Mutex<Option<PathBuf>> = Mutex::new(None);
}

pub fn set_config_dir_override(path: PathBuf) {
    let mut expanded_path = path;
    if let Some(path_str) = expanded_path.to_str() {
        if path_str.starts_with("~/") {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            expanded_path = PathBuf::from(home).join(&path_str[2..]);
        }
    }
    
    if let Ok(mut guard) = CONFIG_DIR_OVERRIDE.lock() {
        *guard = Some(expanded_path);
    }
}

pub fn get_config_dir_override() -> Option<PathBuf> {
    CONFIG_DIR_OVERRIDE.lock().ok()?.clone()
}

pub fn is_config_dir_overridden() -> bool {
    CONFIG_DIR_OVERRIDE.lock().map(|g| g.is_some()).unwrap_or(false)
}

pub fn get_user_cache_dir(host_dir: Option<&str>) -> PathBuf {
    if is_config_dir_overridden() {
        // Return a path that is unlikely to exist and should not be used for state
        return PathBuf::from("/tmp/sr3rs_no_state_cache");
    }

    let mut base = ProjectDirs::from("ca.gc.science", "sarracenia", "sr3rs")
        .map(|d| d.cache_dir().to_path_buf())
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".cache").join("sr3rs")
        });
    
    if let Some(h) = host_dir {
        base = base.join(h);
    }
    base
}

pub fn get_user_config_dir() -> PathBuf {
    if let Some(overridden) = get_config_dir_override() {
        return overridden;
    }

    ProjectDirs::from("ca.gc.science", "sarracenia", "sr3rs")
        .map(|d| d.config_dir().to_path_buf())
        .unwrap_or_else(|| {
            let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
            PathBuf::from(home).join(".config").join("sr3rs")
        })
}

pub fn get_log_dir(host_dir: Option<&str>) -> PathBuf {
    get_user_cache_dir(host_dir).join("log")
}

pub fn get_log_filename(host_dir: Option<&str>, component: &str, configuration: Option<&str>, instance: u32) -> PathBuf {
    let config_part = configuration
        .map(|c| format!("_{}", c.trim_end_matches(".conf")))
        .unwrap_or_default();
    
    get_log_dir(host_dir).join(format!("{}{}_{:02}.log", component, config_part, instance))
}

pub fn get_pid_filename(host_dir: Option<&str>, component: &str, configuration: Option<&str>, instance: u32) -> PathBuf {
    let config_part = configuration
        .map(|c| c.trim_end_matches(".conf"))
        .unwrap_or("unknown");
    
    get_user_cache_dir(host_dir)
        .join(component)
        .join(config_part)
        .join(format!("{}_{}_{:02}.pid", component, config_part, instance))
}

pub fn get_metrics_dir(host_dir: Option<&str>) -> PathBuf {
    get_user_cache_dir(host_dir).join("metrics")
}

pub fn get_metrics_filename(host_dir: Option<&str>, component: &str, configuration: Option<&str>, instance: u32) -> PathBuf {
    let config_part = configuration
        .map(|c| format!("_{}", c.trim_end_matches(".conf")))
        .unwrap_or_default();
    
    get_metrics_dir(host_dir).join(format!("{}{}_{:02}.json", component, config_part, instance))
}
