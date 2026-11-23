//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::flowcb::FlowCB;
use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::path::{Path, PathBuf};

pub struct SmcDownloadCpPlugin {
    config: Config,
}

impl SmcDownloadCpPlugin {
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
        }
    }
}

#[async_trait]
impl FlowCB for SmcDownloadCpPlugin {
    fn name(&self) -> &str {
        "smc_download_cp"
    }

    async fn download(&self, m: &Message, local_file: &Path) -> anyhow::Result<Option<u64>> {
        let cmd_template = self.config.options.get("download_cp_command")
            .or_else(|| self.config.options.get("accelCpCommand"))
            .or_else(|| self.config.options.get("accel_cp_command"))
            .cloned()
            .unwrap_or_else(|| "cp --preserve=timestamps".to_string());

        let url_res = url::Url::parse(&m.base_url);
        let raw_path = if let Ok(u) = &url_res {
            u.path().to_string()
        } else {
            m.base_url.clone()
        };
        let clean_raw = raw_path.trim_start_matches('/');
        let clean_rel = m.rel_path.trim_start_matches('/');

        let combined_path = if clean_raw.is_empty() {
            clean_rel.to_string()
        } else if clean_rel.starts_with(clean_raw) {
            clean_rel.to_string()
        } else {
            format!("{}/{}", clean_raw, clean_rel)
        };

        let source_path = {
            let p_abs = PathBuf::from("/").join(&combined_path);
            if p_abs.exists() {
                p_abs
            } else if let Ok(home) = std::env::var("HOME") {
                let p_home = PathBuf::from(&home).join(&combined_path);
                if p_home.exists() {
                    p_home
                } else {
                    p_abs
                }
            } else {
                p_abs
            }
        };

        if let Some(parent) = local_file.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let arg1 = source_path.to_string_lossy().to_string();
        let arg2 = local_file.to_string_lossy().to_string();

        let cmd_str = if cmd_template.contains("%s") && cmd_template.contains("%d") {
            cmd_template.replace("%s", &arg1).replace("%d", &arg2)
        } else {
            format!("{} {} {}", cmd_template, arg1, arg2)
        };

        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(None);
        }

        ::log::info!("SmcDownloadCpPlugin executing: {}", cmd_str);
        let status = std::process::Command::new(parts[0])
            .args(&parts[1..])
            .status()
            .map_err(|e| anyhow::anyhow!("Failed to execute download_cp command '{}': {}", cmd_str, e))?;

        if status.success() {
            let metadata = std::fs::metadata(local_file)?;
            Ok(Some(metadata.len()))
        } else {
            Err(anyhow::anyhow!("download_cp command failed with exit status {:?}: {}", status.code(), cmd_str))
        }
    }
}
