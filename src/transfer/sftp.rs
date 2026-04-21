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
use ssh2_config_rs::SshConfig;
use ssh2_config_rs::ParseRule;
use std::io::{Read, Write};
use std::path::PathBuf;

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
        let mut host = url.host_str().ok_or_else(|| anyhow::anyhow!("No host in URL: {}", url_str))?.to_string();
        let mut port = url.port().unwrap_or(22);
        let mut user = url.username().to_string();
        let password = url.password();

        // 1. Try to parse SSH config
        let home_dir = dirs::home_dir().unwrap_or_else(|| PathBuf::from("/"));
        let mut identity_files: Vec<String> = Vec::new();

        if let Ok(ssh_config) = SshConfig::parse_default_file(ParseRule::ALLOW_UNKNOWN_FIELDS) {
            let params = ssh_config.query(&host);
            
            if let Some(hostname) = params.host_name {
                host = hostname;
            }
            if let Some(p) = params.port {
                port = p;
            }
            if let Some(u) = params.user {
                if user.is_empty() {
                    user = u;
                }
            }
            if let Some(ids) = params.identity_file {
                // Identity files from ssh config can sometimes have multiple entries per host block
                // but SshConfig might return a single PathBuf, or a Vec<PathBuf> depending on library version.
                // Assuming `ids` is a `Vec<String>` or similar based on docs. If it's a Vec<PathBuf> or Option<Vec<String>>
                // Let's handle it safely by parsing whatever it gives us as strings.
                
                // Let's assume ssh2-config-rs returns `Option<Vec<String>>` or `Option<String>`
                // We'll map whatever it is into our identity_files vec.
                
                // Actually ssh2_config_rs returns a `Option<Vec<String>>` for identity_file.
                for id in ids {
                    identity_files.push(id.to_string_lossy().to_string());
                }
            }
        }

        // 2. Override with local SR3 credentials if found and URL had missing info
        let (final_user, final_pass) = if user.is_empty() || password.is_none() {
            if let Some(cred) = self.config.credentials.get(url_str) {
                (cred.url.username().to_string(), cred.url.password().map(|s| s.to_string()))
            } else {
                (user, password.map(|s| s.to_string()))
            }
        } else {
            (user, password.map(|s| s.to_string()))
        };
        
        let final_user = if final_user.is_empty() {
            std::env::var("USER").unwrap_or_else(|_| "root".to_string())
        } else {
            final_user
        };

        log::debug!("SFTP Connect to {}:{} as user {}", host, port, final_user);

        let tcp = TcpStream::connect(format!("{}:{}", host, port))?;
        let mut sess = Session::new()?;
        sess.set_tcp_stream(tcp);
        sess.handshake()?;

        if let Some(pass) = final_pass {
            sess.userauth_password(&final_user, &pass)?;
        } else {
            // Try agent first
            if sess.userauth_agent(&final_user).is_err() {
                let mut authenticated = false;
                
                // Try identities from ssh config
                for id_file in identity_files {
                    let path = if id_file.starts_with("~/") {
                        home_dir.join(id_file.trim_start_matches("~/"))
                    } else {
                        PathBuf::from(id_file)
                    };
                    
                    if path.exists() {
                        if sess.userauth_pubkey_file(&final_user, None, &path, None).is_ok() {
                            authenticated = true;
                            break;
                        }
                    }
                }
                
                // If not authenticated, try standard default keys
                if !authenticated {
                    let ssh_dir = home_dir.join(".ssh");
                    let ed25519 = ssh_dir.join("id_ed25519");
                    let rsa = ssh_dir.join("id_rsa");
                    let dsa = ssh_dir.join("id_dsa");
                    
                    if ed25519.exists() && sess.userauth_pubkey_file(&final_user, None, &ed25519, None).is_ok() {
                        // success
                    } else if rsa.exists() && sess.userauth_pubkey_file(&final_user, None, &rsa, None).is_ok() {
                        // success
                    } else if dsa.exists() && sess.userauth_pubkey_file(&final_user, None, &dsa, None).is_ok() {
                        // success
                    }
                }
            }
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

    async fn mkdir(&self, remote_dir: &str) -> anyhow::Result<()> {
        let remote_dir_str = remote_dir.to_string();
        let target_url = if let Some(st) = &self.config.send_to {
            st.clone()
        } else {
            // Technically base_url of message but we only have self.config here.
            // If sender is calling this, send_to should be set. If not, error out early or fall back.
            anyhow::bail!("mkdir requires sendTo configured for SFTP")
        };

        let self_clone = Self::new(&self.config);
        tokio::task::spawn_blocking(move || {
            let sess = self_clone.connect_to(&target_url)?;
            let sftp = sess.sftp()?;
            
            let remote_p = Path::new(&remote_dir_str);
            
            // Basic mkdir -p equivalent for remote path
            let mut current = std::path::PathBuf::new();
            for part in remote_p.components() {
                current.push(part);
                let _ = sftp.mkdir(&current, 0o775); // ignore error if it already exists
            }
            
            Ok(())
        }).await?
    }
}
