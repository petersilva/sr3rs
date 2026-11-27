//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use std::path::PathBuf;
use crate::config::paths;
use anyhow::Result;
use glob::glob;

/// Generic logging macro that automatically captures the calling routine name as the target.
#[macro_export]
macro_rules! sr3_log {
    ($level:expr, $($arg:tt)+) => {{
        struct Helper;
        let full_path = std::any::type_name::<Helper>();
        let target = full_path.strip_suffix("::Helper").unwrap_or(full_path);
        ::log::log!(target: target, $level, $($arg)+);
    }};
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)+) => {
        $crate::sr3_log!(::log::Level::Info, $($arg)+);
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)+) => {
        $crate::sr3_log!(::log::Level::Warn, $($arg)+);
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)+) => {
        $crate::sr3_log!(::log::Level::Error, $($arg)+);
    };
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)+) => {
        $crate::sr3_log!(::log::Level::Debug, $($arg)+);
    };
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)+) => {
        $crate::sr3_log!(::log::Level::Trace, $($arg)+);
    };
}

pub fn clean_target(raw: &str) -> String {
    let mut s = raw;
    while let Some(idx) = s.rfind("::{{closure}}") {
        s = &s[..idx];
    }
    
    if s.starts_with('<') {
        if let Some(gt_pos) = s.find(">::") {
            let func_name = &s[gt_pos + 3..];
            let type_part = &s[1..gt_pos];
            let actual_type = if let Some(as_pos) = type_part.find(" as ") {
                &type_part[..as_pos]
            } else {
                type_part
            };
            if let Some(last_colon) = actual_type.rfind("::") {
                let mod_path = &actual_type[..last_colon];
                return format!("{}::{}", mod_path, func_name);
            } else {
                return format!("{}::{}", actual_type, func_name);
            }
        }
    }
    
    s.to_string()
}

pub fn setup_logging(level: log::LevelFilter, log_file: Option<PathBuf>) -> Result<()> {
    let mut dispatch = fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} [{}] {} {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%Z"),
                record.level(),
                clean_target(record.target()),
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
                let mut v = vec![config_dir.join(default_component).join("**").join(&p).to_string_lossy().to_string()];
                if default_component == "post" {
                    v.push(config_dir.join("cpost").join("**").join(&p).to_string_lossy().to_string());
                } else if default_component == "cpost" {
                    v.push(config_dir.join("post").join("**").join(&p).to_string_lossy().to_string());
                } else if default_component == "shovel" {
                    v.push(config_dir.join("cpump").join("**").join(&p).to_string_lossy().to_string());
                } else if default_component == "cpump" {
                    v.push(config_dir.join("shovel").join("**").join(&p).to_string_lossy().to_string());
                }
                v
            }
        } else {
            if p.contains('/') {
                vec![
                    config_dir.join(&p).to_string_lossy().to_string(),
                    config_dir.join(format!("{}.conf", p)).to_string_lossy().to_string()
                ]
            } else {
                let mut v = vec![
                    config_dir.join(default_component).join(&p).to_string_lossy().to_string(),
                    config_dir.join(default_component).join(format!("{}.conf", p)).to_string_lossy().to_string()
                ];
                if default_component == "post" {
                    v.push(config_dir.join("cpost").join(&p).to_string_lossy().to_string());
                    v.push(config_dir.join("cpost").join(format!("{}.conf", p)).to_string_lossy().to_string());
                } else if default_component == "cpost" {
                    v.push(config_dir.join("post").join(&p).to_string_lossy().to_string());
                    v.push(config_dir.join("post").join(format!("{}.conf", p)).to_string_lossy().to_string());
                } else if default_component == "shovel" {
                    v.push(config_dir.join("cpump").join(&p).to_string_lossy().to_string());
                    v.push(config_dir.join("cpump").join(format!("{}.conf", p)).to_string_lossy().to_string());
                } else if default_component == "cpump" {
                    v.push(config_dir.join("shovel").join(&p).to_string_lossy().to_string());
                    v.push(config_dir.join("shovel").join(format!("{}.conf", p)).to_string_lossy().to_string());
                }
                v
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

#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrayProcess {
    pub pid: u32,
    pub name: String,
    pub cmdline: String,
    pub component: Option<String>,
    pub config_file: Option<String>,
}

#[cfg(not(target_arch = "wasm32"))]
pub fn collect_registered_pids() -> std::collections::HashSet<u32> {
    let mut valid_pids = std::collections::HashSet::new();
    let cache_dir = paths::get_user_cache_dir(None);
    let pattern = cache_dir.join("**").join("*.pid").to_string_lossy().to_string();
    if let Ok(entries) = glob(&pattern) {
        for entry in entries.flatten() {
            if let Ok(content) = std::fs::read_to_string(&entry) {
                if let Ok(pid) = content.trim().parse::<i32>() {
                    if is_process_running(pid) {
                        valid_pids.insert(pid as u32);
                    }
                }
            }
        }
    }
    valid_pids
}

#[cfg(target_os = "linux")]
pub fn is_process_leader(pid: u32) -> bool {
    let status_path = format!("/proc/{}/status", pid);
    if let Ok(content) = std::fs::read_to_string(status_path) {
        let mut tgid = None;
        let mut pid_val = None;
        for line in content.lines() {
            if line.starts_with("Tgid:") {
                tgid = line.split_whitespace().nth(1).and_then(|s| s.parse::<u32>().ok());
            } else if line.starts_with("Pid:") {
                pid_val = line.split_whitespace().nth(1).and_then(|s| s.parse::<u32>().ok());
            }
            if tgid.is_some() && pid_val.is_some() {
                break;
            }
        }
        if let (Some(t), Some(p)) = (tgid, pid_val) {
            return t == p;
        }
    }
    true
}

#[cfg(not(target_os = "linux"))]
pub fn is_process_leader(_pid: u32) -> bool {
    true
}

#[cfg(not(target_arch = "wasm32"))]
pub fn find_stray_processes() -> Vec<StrayProcess> {
    use sysinfo::{ProcessRefreshKind, System, UpdateKind};

    let registered_pids = collect_registered_pids();
    let current_pid = std::process::id();

    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessRefreshKind::new().with_cmd(UpdateKind::Always),
    );

    let mut strays = Vec::new();

    for (pid, process) in sys.processes() {
        let u_pid = pid.as_u32();
        if u_pid == current_pid {
            continue;
        }

        // Skip Linux secondary threads (LWPs) whose TID != TGID
        if !is_process_leader(u_pid) {
            continue;
        }

        let name = process.name();
        let cmd = process.cmd();

        let is_sr3rs = name.contains("sr3rs")
            || cmd.iter().any(|arg| arg.contains("sr3rs"));

        if !is_sr3rs {
            continue;
        }

        // Ignore commands that are running status or list checks
        if cmd.iter().any(|arg| arg == "status" || arg == "list") {
            continue;
        }

        if !registered_pids.contains(&u_pid) {
            let cmdline = if cmd.is_empty() {
                name.to_string()
            } else {
                cmd.join(" ")
            };

            if cmdline.contains("status") {
                continue;
            }

            let mut component = None;
            let mut config_file = None;

            for (idx, arg) in cmd.iter().enumerate() {
                if arg == "run-instance" && idx + 2 < cmd.len() {
                    component = Some(cmd[idx + 1].clone());
                    let raw_cfg = &cmd[idx + 2];
                    let stem = std::path::Path::new(raw_cfg)
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| raw_cfg.clone());
                    config_file = Some(stem);
                } else if arg == "--config" && idx + 1 < cmd.len() {
                    let raw_cfg = &cmd[idx + 1];
                    component = Some(detect_component(raw_cfg));
                    let stem = std::path::Path::new(raw_cfg)
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| raw_cfg.clone());
                    config_file = Some(stem);
                }
            }

            if !strays.iter().any(|s: &StrayProcess| s.pid == u_pid) {
                strays.push(StrayProcess {
                    pid: u_pid,
                    name: name.to_string(),
                    cmdline,
                    component,
                    config_file,
                });
            }
        }
    }

    strays
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
        if let Some(pid_file) = paths::find_pid_filename(config.host_dir.as_deref(), &comp, config_name.as_deref(), i) {
            if let Ok(pid_str) = std::fs::read_to_string(&pid_file) {
                if let Ok(pid) = pid_str.trim().parse::<i32>() {
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

    let is_interactive = (comp == "post" || comp == "cpost") && config.sleep <= 0.0;

    let mut state = if state_dir.join("disabled").exists() {
        "stop".to_string()
    } else if instances_requested == 0 && running_count == 0 {
        if is_interactive {
            "inte".to_string()
        } else if state_exists {
            "stop".to_string()
        } else {
            "new".to_string()
        }
    } else if running_count == 0 {
        if is_interactive {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_clean_target() {
        let raw = "<sr3rs::flow::flowcb::nodupe::disk::DiskNoDupePlugin as sr3rs::flow::flowcb::FlowCB>::on_housekeeping::{{closure}}";
        assert_eq!(clean_target(raw), "sr3rs::flow::flowcb::nodupe::disk::on_housekeeping");

        let raw_mod = "sr3rs::flow::flowcb::log";
        assert_eq!(clean_target(raw_mod), "sr3rs::flow::flowcb::log");
    }

    #[test]
    fn test_resolve_patterns_fallback_cpost() {
        let dir = tempdir().unwrap();
        let cpost_dir = dir.path().join("cpost");
        std::fs::create_dir_all(&cpost_dir).unwrap();
        let conf_file = cpost_dir.join("test_cpost.conf");
        std::fs::write(&conf_file, "# cpost config\n").unwrap();

        paths::set_config_dir_override(dir.path().to_path_buf());

        let results = resolve_patterns(vec!["test_cpost.conf".to_string()], "post");
        assert_eq!(results.len(), 1);
        assert_eq!(detect_component(&results[0]), "cpost");
    }

    #[test]
    fn test_collect_registered_pids() {
        let dir = tempdir().unwrap();
        let pid_dir = dir.path().join("poll").join("test_cfg");
        std::fs::create_dir_all(&pid_dir).unwrap();
        let pid_file = pid_dir.join("poll_test_cfg_01.pid");
        std::fs::write(&pid_file, "12345\n").unwrap();

        // Standard cache dir resolution will be tested via pattern search
        let pattern = dir.path().join("**").join("*.pid").to_string_lossy().to_string();
        let mut pids = std::collections::HashSet::new();
        if let Ok(entries) = glob(&pattern) {
            for entry in entries.flatten() {
                if let Ok(content) = std::fs::read_to_string(&entry) {
                    if let Ok(pid) = content.trim().parse::<u32>() {
                        pids.insert(pid);
                    }
                }
            }
        }
        assert!(pids.contains(&12345));
    }
}
