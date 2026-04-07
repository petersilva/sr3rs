//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::path::Path;

pub mod http;

#[async_trait]
pub trait Transfer: Send + Sync {
    async fn get(&self, msg: &Message, local_file: &Path) -> anyhow::Result<u64>;
}

pub fn get_transfer(scheme: &str, _config: &Config) -> Option<Box<dyn Transfer>> {
    match scheme {
        "http" | "https" => Some(Box::new(http::HttpTransfer::new())),
        // "sftp" => Some(Box::new(sftp::SftpTransfer::new(config))),
        _ => None,
    }
}
