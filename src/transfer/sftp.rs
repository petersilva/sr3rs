//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::message::Message;
use crate::transfer::Transfer;
use crate::Config;
use async_trait::async_trait;
use std::path::Path;
use std::net::TcpStream;
use ssh2::Session;
use std::io::{Read, Write};

pub struct SftpTransfer {
    config: Config,
}

impl SftpTransfer {
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
        }
    }

    fn connect(&self, msg: &Message) -> anyhow::Result<Session> {
        let url = url::Url::parse(&msg.base_url)?;
        let host = url.host_str().ok_or_else(|| anyhow::anyhow!("No host in base_url"))?;
        let port = url.port().unwrap_or(22);
        let user = url.username();
        let password = url.password();

        let tcp = TcpStream::connect(format!("{}:{}", host, port))?;
        let mut sess = Session::new()?;
        sess.set_tcp_stream(tcp);
        sess.handshake()?;

        // Try to find credentials if not in URL
        let (final_user, final_pass) = if user.is_empty() || password.is_none() {
            if let Some(cred) = self.config.credentials.get(&msg.base_url) {
                (cred.url.username().to_string(), cred.url.password().map(|s| s.to_string()))
            } else {
                (user.to_string(), password.map(|s| s.to_string()))
            }
        } else {
            (user.to_string(), password.map(|s| s.to_string()))
        };

        if let Some(pass) = final_pass {
            sess.userauth_password(&final_user, &pass)?;
        } else {
            // Try agent or default keys? For now just try agent.
            sess.userauth_agent(&final_user)?;
        }

        if !sess.authenticated() {
            anyhow::bail!("Authentication failed for sftp://{}@{}", final_user, host);
        }

        Ok(sess)
    }
}

#[async_trait]
impl Transfer for SftpTransfer {
    async fn get(&self, msg: &Message, local_file: &Path) -> anyhow::Result<u64> {
        let rel_path = msg.rel_path.clone();
        let local_path = local_file.to_path_buf();
        
        // Since ssh2 is blocking, we use spawn_blocking
        let sess = self.connect(msg)?;
        
        tokio::task::spawn_blocking(move || {
            let sftp = sess.sftp()?;
            let mut remote_file = sftp.open(Path::new(&rel_path))?;
            
            // Ensure parent directory exists
            if let Some(parent) = local_path.parent() {
                std::fs::create_dir_all(parent)?;
            }

            let mut local_f = std::fs::File::create(&local_path)?;
            let mut buffer = [0; 16384];
            let mut downloaded = 0;

            loop {
                let bytes_read = remote_file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                local_f.write_all(&buffer[..bytes_read])?;
                downloaded += bytes_read as u64;
            }

            Ok(downloaded)
        }).await?
    }
}
