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

pub fn resolve_patterns(patterns: Vec<String>, default_component: &str) -> Vec<String> {
    let config_dir = paths::get_user_config_dir();
    
    let mut results = Vec::new();
    if patterns.is_empty() {
        let p = config_dir.join("**").join("*.conf").to_string_lossy().to_string();
        if let Ok(entries) = glob(&p) {
            for entry in entries.flatten() {
                if entry.is_file() {
                    let path_str = entry.to_string_lossy().to_string();
                    if !is_global_config(&path_str) && !results.contains(&path_str) {
                        results.push(path_str);
                    }
                }
            }
        }
        return results;
    }

    for p in patterns {
        let p_path = std::path::Path::new(&p);
        if p_path.exists() && p_path.is_file() && ! is_global_config(&p) {
            if !results.contains(&p) {
                results.push(p.clone());
            }
            continue;
        }

        let search_patterns = if p.contains('*') {
            if p.contains('/') {
                vec![config_dir.join(&p).to_string_lossy().to_string()]
            } else {
                vec![config_dir.join("**").join(&p).to_string_lossy().to_string()]
            }
        } else {
            if p.contains('/') {
                vec![
                    config_dir.join(&p).to_string_lossy().to_string(),
                    config_dir.join(format!("{}.conf", p)).to_string_lossy().to_string()
                ]
            } else {
                vec![
                    config_dir.join(default_component).join(&p).to_string_lossy().to_string(),
                    config_dir.join(default_component).join(format!("{}.conf", p)).to_string_lossy().to_string()
                ]
            }
        };

        for sp in search_patterns {
            if let Ok(entries) = glob(&sp) {
                for entry in entries.flatten() {
                    if entry.is_file() {
                        let path_str = entry.to_string_lossy().to_string();
                        if !is_global_config(&path_str) && !results.contains(&path_str) {
                            results.push(path_str);
                        }
                    }
                }
            }
        }
    }
    
    results
}

pub fn detect_component_from_config(config: &crate::Config) -> String {
    if let Some(ref path) = config.config_search_paths.first() {
        return detect_component(&path.to_string_lossy());
    }
    "subscribe".to_string()
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

pub fn redact_url(url_str: &str) -> String {
    if let Ok(mut url) = url::Url::parse(url_str) {
        if url.password().is_some() {
            let _ = url.set_password(Some(""));
        }
        url.to_string()
    } else {
        url_str.to_string()
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub fn get_state(config_file: &str) -> String {
    let comp = detect_component(config_file);
    let mut config = crate::Config::new();
    config.apply_component_defaults(&comp);

    let config_loaded = config.load(config_file).is_ok();
    let config_finalized = if config_loaded { config.finalize().is_ok() } else { false };

    let config_name = if config_loaded {
        config.configname.clone()
    } else {
        std::path::Path::new(config_file)
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
    };

    let mut running_count = 0;
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
        "stop".to_string()
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
    
    state
}
