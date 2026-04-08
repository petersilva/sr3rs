//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use std::path::PathBuf;
use crate::config::paths;
use anyhow::Result;
use glob::glob;

pub fn setup_logging(level: log::LevelFilter, log_file: Option<PathBuf>) -> Result<()> {
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

pub fn detect_component(config_path: &str) -> String {
    let config_dir = paths::get_user_config_dir();
    let path = std::path::Path::new(config_path);
    
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };

    if let Ok(rel) = abs_path.strip_prefix(&config_dir) {
        if let Some(first) = rel.components().next() {
            let comp_str = first.as_os_str().to_string_lossy();
            match comp_str.as_ref() {
                "subscribe" | "poll" | "post" | "watch" | "winnow" | "shovel" | "sender" | "cpost" | "cpump" | "report" | "sarra" => {
                    return comp_str.to_string();
                }
                _ => {}
            }
        }
    }

    let parts: Vec<&str> = config_path.split('/').collect();
    for i in (0..parts.len()).rev() {
        match parts[i] {
            "subscribe" | "poll" | "post" | "watch" | "winnow" | "shovel" | "sender" | "cpost" | "cpump" | "report" | "sarra" => {
                return parts[i].to_string();
            }
            _ => {}
        }
    }

    "subscribe".to_string()
}

pub fn resolve_patterns(pattern: Option<String>) -> Vec<String> {
    let config_dir = paths::get_user_config_dir();
    
    let mut results = Vec::new();
    
    if let Some(ref p) = pattern {
        let p_path = std::path::Path::new(p);
        if p_path.exists() && p_path.is_file() {
            results.push(p.clone());
            return results;
        }
    }

    let search_patterns = match pattern {
        Some(ref p) => {
            if p.contains('*') {
                if p.contains('/') {
                    vec![config_dir.join(p).to_string_lossy().to_string()]
                } else {
                    vec![config_dir.join("**").join(p).to_string_lossy().to_string()]
                }
            } else {
                if p.contains('/') {
                    vec![
                        config_dir.join(p).to_string_lossy().to_string(),
                        config_dir.join(format!("{}.conf", p)).to_string_lossy().to_string()
                    ]
                } else {
                    vec![
                        config_dir.join("**").join(p).to_string_lossy().to_string(),
                        config_dir.join("**").join(format!("{}.conf", p)).to_string_lossy().to_string()
                    ]
                }
            }
        }
        None => vec![config_dir.join("**").join("*.conf").to_string_lossy().to_string()],
    };

    for p in search_patterns {
        log::debug!("Searching for configs with pattern: {}", p);
        if let Ok(entries) = glob(&p) {
            for entry in entries.flatten() {
                if entry.is_file() {
                    let path_str = entry.to_string_lossy().to_string();
                    if !results.contains(&path_str) {
                        results.push(path_str);
                    }
                }
            }
        }
    }
    
    results
}

pub fn is_process_running(pid: i32) -> bool {
    if pid <= 0 { return false; }
    unsafe { libc::kill(pid, 0) == 0 }
}

pub fn is_global_config(path_str: &str) -> bool {
    let path = std::path::Path::new(path_str);
    if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
        return filename == "admin.conf" || filename == "default.conf" || filename == "credentials.conf";
    }
    false
}
