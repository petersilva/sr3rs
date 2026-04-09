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

    fn connect_to(&self, url_str: &str) -> anyhow::Result<Session> {
        let url = url::Url::parse(url_str)?;
        let host = url.host_str().ok_or_else(|| anyhow::anyhow!("No host in URL: {}", url_str))?;
        let port = url.port().unwrap_or(22);
        let user = url.username();
        let password = url.password();

        let tcp = TcpStream::connect(format!("{}:{}", host, port))?;
        let mut sess = Session::new()?;
        sess.set_tcp_stream(tcp);
        sess.handshake()?;

        // Try to find credentials if not in URL
        let (final_user, final_pass) = if user.is_empty() || password.is_none() {
            if let Some(cred) = self.config.credentials.get(url_str) {
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
        let base_url = msg.base_url.clone();
        
        let self_clone = Self::new(&self.config);
        tokio::task::spawn_blocking(move || {
            let sess = self_clone.connect_to(&base_url)?;
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

    async fn put(&self, msg: &Message, local_file: &Path, remote_file_name: &str) -> anyhow::Result<u64> {
        let local_path = local_file.to_path_buf();
        let remote_file_str = remote_file_name.to_string();
        
        let target_url = if let Some(st) = &self.config.send_to {
            st.clone()
        } else {
            msg.base_url.clone()
        };

        let self_clone = Self::new(&self.config);
        tokio::task::spawn_blocking(move || {
            let sess = self_clone.connect_to(&target_url)?;
            let sftp = sess.sftp()?;
            
            let remote_p = Path::new(&remote_file_str);
            
            // Basic mkdir -p equivalent for remote path
            if let Some(parent) = remote_p.parent() {
                let mut current = std::path::PathBuf::new();
                for part in parent.components() {
                    current.push(part);
                    // SFTP mkdir is not recursive and fails if it already exists
                    let _ = sftp.mkdir(&current, 0o775);
                }
            }
            
            let mut remote_f = sftp.create(remote_p)?;
            let mut local_f = std::fs::File::open(&local_path)?;
            
            let mut buffer = [0; 16384];
            let mut uploaded = 0;

            loop {
                let bytes_read = local_f.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                remote_f.write_all(&buffer[..bytes_read])?;
                uploaded += bytes_read as u64;
            }

            Ok(uploaded)
        }).await?
    }
}
