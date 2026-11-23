//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::message::Message;
use crate::Config;
use crate::transfer::Transfer;
use async_trait::async_trait;
use std::path::Path;
use std::fs;
use std::process::Command;

pub struct FileTransfer {
    config: Config,
}

impl FileTransfer {
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
        }
    }

    fn get_accelerated(&self, source_path: &Path, local_file: &Path) -> anyhow::Result<Option<u64>> {
        let cmd_template = match self.config.options.get("accelCpCommand")
            .or_else(|| self.config.options.get("accel_cp_command"))
            .or_else(|| self.config.options.get("download_cp_command"))
        {
            Some(tpl) => tpl.clone(),
            None => {
                if self.config.accel_threshold > 0 {
                    if let Ok(meta) = fs::metadata(source_path) {
                        if meta.len() >= self.config.accel_threshold as u64 {
                            "/bin/cp %s %d".to_string()
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                } else {
                    return Ok(None);
                }
            }
        };

        let arg1 = source_path.to_string_lossy().to_string();
        let arg2 = local_file.to_string_lossy().to_string();
        let cmd_str = cmd_template.replace("%s", &arg1).replace("%d", &arg2);
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        if parts.is_empty() {
            return Ok(None);
        }
        log::info!("accel_cp: {}", cmd_str);
        let status = Command::new(parts[0])
            .args(&parts[1..])
            .status()
            .map_err(|e| anyhow::anyhow!("Failed to execute accel_cp command '{}': {}", cmd_str, e))?;

        if status.success() {
            let metadata = fs::metadata(local_file)?;
            Ok(Some(metadata.len()))
        } else {
            Err(anyhow::anyhow!("accel_cp command failed with exit status {:?}: {}", status.code(), cmd_str))
        }
    }
}

#[async_trait]
impl Transfer for FileTransfer {
    async fn get(&self, msg: &Message, local_file: &Path) -> anyhow::Result<u64> {
        let remote_url = url::Url::parse(&msg.base_url)?;
        let remote_path = remote_url.path(); // Don't trim the leading slash for absolute file URLs!
        
        let source_path = if remote_path.starts_with('/') {
            Path::new(remote_path).join(&msg.rel_path)
        } else {
            // Fallback for relative paths, though file:// URLs typically specify absolute paths.
            Path::new("/").join(remote_path).join(&msg.rel_path)
        };

        if !source_path.exists() {
            return Err(anyhow::anyhow!("Source file not found: {}", source_path.display()));
        }

        if let Some(parent) = local_file.parent() {
            fs::create_dir_all(parent)?;
        }

        if let Some(size) = self.get_accelerated(&source_path, local_file)? {
            return Ok(size);
        }

        fs::copy(&source_path, local_file)?;
        let metadata = fs::metadata(local_file)?;
        Ok(metadata.len())
    }

    async fn put(&self, _msg: &Message, _local_file: &Path, _remote_file: &str) -> anyhow::Result<u64> {
        // FIXME: implement put for local file if needed (sender to local)
        Err(anyhow::anyhow!("PUT not implemented for FileTransfer"))
    }

    async fn mkdir(&self, remote_dir: &str) -> anyhow::Result<()> {
        let path = Path::new(remote_dir);
        if !path.exists() {
            fs::create_dir_all(path)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_transfer_standard_and_accel() {
        let dir = tempdir().unwrap();
        let src_path = dir.path().join("source.txt");
        let dst_path = dir.path().join("dest.txt");

        fs::write(&src_path, "hello world").unwrap();

        let mut config = Config::default();
        let transfer = FileTransfer::new(&config);

        let msg = Message::new(&format!("file://{}", dir.path().display()), "source.txt");

        // Standard copy
        let size = transfer.get(&msg, &dst_path).await.unwrap();
        assert_eq!(size, 11);
        assert_eq!(fs::read_to_string(&dst_path).unwrap(), "hello world");

        // Accelerated copy using cp
        let dst_path2 = dir.path().join("dest2.txt");
        config.options.insert("accelCpCommand".to_string(), "cp %s %d".to_string());
        let transfer_accel = FileTransfer::new(&config);
        let size2 = transfer_accel.get(&msg, &dst_path2).await.unwrap();
        assert_eq!(size2, 11);
        assert_eq!(fs::read_to_string(&dst_path2).unwrap(), "hello world");
    }
}
