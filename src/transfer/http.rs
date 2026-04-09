//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::message::Message;
use crate::transfer::Transfer;
use async_trait::async_trait;
use std::path::Path;
use tokio::io::AsyncWriteExt;
use futures_util::StreamExt;

pub struct HttpTransfer {
    client: reqwest::Client,
}

impl HttpTransfer {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl Transfer for HttpTransfer {
    async fn get(&self, msg: &Message, local_file: &Path) -> anyhow::Result<u64> {
        let url = format!("{}{}", msg.base_url, msg.rel_path);
        log::debug!("HTTP GET: {}", url);

        let response = self.client.get(&url).send().await?;
        if !response.status().is_success() {
            anyhow::bail!("HTTP request failed with status: {}", response.status());
        }

        // Ensure parent directory exists
        if let Some(parent) = local_file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut file = tokio::fs::File::create(local_file).await?;
        let mut downloaded: u64 = 0;
        let mut stream = response.bytes_stream();

        while let Some(item) = stream.next().await {
            let chunk = item?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
        }

        file.flush().await?;
        Ok(downloaded)
    }

    async fn put(&self, _msg: &Message, _local_file: &Path, _remote_file: &str) -> anyhow::Result<u64> {
        anyhow::bail!("HTTP PUT not implemented")
    }
}
