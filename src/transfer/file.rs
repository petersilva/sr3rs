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

pub struct FileTransfer {
    #[allow(dead_code)]
    config: Config,
}

impl FileTransfer {
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
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
