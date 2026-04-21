//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::message::Message;
use crate::Config;
use async_trait::async_trait;
use std::path::Path;

pub mod http;
pub mod sftp;
pub mod ftp;
pub mod file;

#[async_trait]
pub trait Transfer: Send + Sync {
    async fn get(&self, msg: &Message, local_file: &Path) -> anyhow::Result<u64>;
    async fn put(&self, msg: &Message, local_file: &Path, remote_file: &str) -> anyhow::Result<u64>;
    async fn mkdir(&self, remote_dir: &str) -> anyhow::Result<()>;
}

pub fn get_transfer(scheme: &str, config: &Config) -> Option<Box<dyn Transfer>> {
    match scheme {
        "http" | "https" => Some(Box::new(http::HttpTransfer::new())),
        "sftp" => Some(Box::new(sftp::SftpTransfer::new(config))),
        "ftp" | "ftps" => Some(Box::new(ftp::FtpTransfer::new(config))),
        "file" => Some(Box::new(file::FileTransfer::new(config))),
        _ => None,
    }
}
