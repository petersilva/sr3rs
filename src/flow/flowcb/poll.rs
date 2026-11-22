//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::flow::flowcb::FlowCB;
use crate::flow::Worklist;
use crate::message::Message;
use crate::transfer::sftp::SftpTransfer;
use crate::Config;
use crate::utils::redact_url;
use async_trait::async_trait;
use std::path::{Path, PathBuf};

pub struct PollPlugin {
    pub name: String,
    pub config: Config,
}

impl PollPlugin {
    pub fn new(config: &Config) -> Self {
        Self {
            name: "poll".to_string(),
            config: config.clone(),
        }
    }

    async fn poll_directory_sftp(
        &self,
        sftp: &SftpTransfer,
        base_url: &str,
        remote_path: &Path,
        worklist: &mut Worklist,
    ) -> anyhow::Result<()> {
        let entries = match sftp.readdir(base_url, remote_path).await {
            Ok(e) => e,
            Err(e) => {
                log::warn!("POLL SFTP: Could not list directory {}: {}", remote_path.display(), e);
                return Ok(());
            }
        };

        for (entry_path, stat) in entries {
            let file_name = match entry_path.file_name().and_then(|n| n.to_str()) {
                Some(name) => name,
                None => continue,
            };

            if file_name == "." || file_name == ".." {
                continue;
            }

            let rel_path = entry_path.strip_prefix("/").unwrap_or(&entry_path).to_string_lossy().to_string();
            let mut msg = Message::new(base_url, &rel_path);

            if stat.is_dir() {
                msg.file_operation.insert("directory".to_string(), "".to_string());
                if let Some(mtime) = stat.mtime {
                    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(mtime as i64, 0);
                    if let Some(dt) = dt {
                        msg.fields.insert("mtime".to_string(), dt.to_rfc3339());
                    }
                }
                worklist.incoming.push(msg);

                if self.config.recursive {
                    Box::pin(self.poll_directory_sftp(sftp, base_url, &entry_path, worklist)).await?;
                }
            } else {
                if let Some(size) = stat.size {
                    msg.fields.insert("size".to_string(), size.to_string());
                }
                if let Some(mtime) = stat.mtime {
                    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(mtime as i64, 0);
                    if let Some(dt) = dt {
                        msg.fields.insert("mtime".to_string(), dt.to_rfc3339());
                    }
                }
                msg.identity.insert("method".to_string(), "cod".to_string());
                worklist.incoming.push(msg);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl FlowCB for PollPlugin {
    fn name(&self) -> &str {
        &self.name
    }

    async fn gather(&mut self, worklist: &mut Worklist) -> anyhow::Result<()> {
        let poll_url_str = match &self.config.poll_url {
            Some(u) => u.clone(),
            None => {
                log::warn!("POLL: No pollUrl configured.");
                return Ok(());
            }
        };

        let parsed_url = match url::Url::parse(&poll_url_str) {
            Ok(u) => u,
            Err(e) => {
                log::error!("POLL: Failed to parse poll_url {}: {}", redact_url(&poll_url_str), e);
                return Ok(());
            }
        };

        match parsed_url.scheme() {
            "sftp" => {
                log::info!("POLL SFTP: Polling {}", redact_url(&poll_url_str));
                let sftp = SftpTransfer::new(&self.config);
                
                let base_url = if let Some(host) = parsed_url.host_str() {
                    let user_part = if !parsed_url.username().is_empty() {
                        format!("{}@", parsed_url.username())
                    } else {
                        String::new()
                    };
                    let port_part = if let Some(port) = parsed_url.port() {
                        format!(":{}", port)
                    } else {
                        String::new()
                    };
                    format!("sftp://{}{}{}/", user_part, host, port_part)
                } else {
                    "sftp://localhost/".to_string()
                };

                let path_str = parsed_url.path();
                let remote_path = if !path_str.is_empty() && path_str != "/" {
                    PathBuf::from(path_str)
                } else if !self.config.directory.as_os_str().is_empty() {
                    self.config.directory.clone()
                } else {
                    PathBuf::from("/")
                };
                
                self.poll_directory_sftp(&sftp, &base_url, &remote_path, worklist).await?;
            }
            scheme => {
                log::warn!("POLL: Polling scheme '{}' for {} is not natively implemented yet.", scheme, redact_url(&poll_url_str));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poll_plugin_new() {
        let config = Config::default();
        let plugin = PollPlugin::new(&config);
        assert_eq!(plugin.name, "poll");
    }
}
