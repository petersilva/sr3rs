//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use crate::message::Message;
use crate::transfer::Transfer;
use crate::Config;
use async_trait::async_trait;
use std::path::Path;
use std::process::Command;
use suppaftp::tokio::{AsyncNativeTlsFtpStream, AsyncNativeTlsConnector};
use suppaftp::async_native_tls::TlsConnector;
use suppaftp::types::{FileType, FormatControl};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct FtpTransfer {
    config: Config,
}

impl FtpTransfer {
    pub fn new(config: &Config) -> Self {
        Self {
            config: config.clone(),
        }
    }

    async fn connect(&self, url_str: &str) -> anyhow::Result<AsyncNativeTlsFtpStream> {
        let url = url::Url::parse(url_str)?;
        let host = url.host_str().ok_or_else(|| anyhow::anyhow!("No host in URL: {}", url_str))?;
        
        let cred = self.config.credentials.get(url_str);
        let use_tls = cred.map(|c| c.tls).unwrap_or(false);
        let implicit = cred.map(|c| c.implicit_ftps).unwrap_or(false);

        let port = url.port().unwrap_or(if implicit { 990 } else { 21 });
        let addr = format!("{}:{}", host, port);

        let mut ftp = if implicit {
            let connector = TlsConnector::new();
            AsyncNativeTlsFtpStream::connect_secure_implicit(addr, AsyncNativeTlsConnector::from(connector), host).await?
        } else {
            let mut ftp = AsyncNativeTlsFtpStream::connect(addr).await?;
            if use_tls {
                let connector = TlsConnector::new();
                ftp = ftp.into_secure(AsyncNativeTlsConnector::from(connector), host).await?;
            }
            ftp
        };

        self.setup_ftp(&mut ftp, url_str).await?;
        Ok(ftp)
    }

    async fn setup_ftp(&self, ftp: &mut AsyncNativeTlsFtpStream, url_str: &str) -> anyhow::Result<()> {
        let url = url::Url::parse(url_str)?;
        let cred = self.config.credentials.get(url_str);
        
        let (user, pass, passive, binary) = if let Some(c) = cred {
            (
                if !c.url.username().is_empty() { c.url.username().to_string() } else { "anonymous".to_string() },
                c.url.password().map(|s| s.to_string()).unwrap_or_else(|| "anonymous".to_string()),
                c.passive,
                c.binary,
            )
        } else {
            (
                if !url.username().is_empty() { url.username().to_string() } else { "anonymous".to_string() },
                url.password().map(|s| s.to_string()).unwrap_or_else(|| "anonymous".to_string()),
                true,
                true,
            )
        };

        ftp.login(&user, &pass).await?;

        if passive {
            ftp.set_mode(suppaftp::Mode::Passive);
        } else {
            ftp.set_mode(suppaftp::Mode::Active);
        }

        if binary {
            ftp.transfer_type(FileType::Binary).await?;
        } else {
            ftp.transfer_type(FileType::Ascii(FormatControl::Default)).await?;
        }

        Ok(())
    }

    async fn cd_forced(&self, ftp: &mut AsyncNativeTlsFtpStream, path: &str) -> anyhow::Result<()> {
        if path.is_empty() || path == "." {
            return Ok(());
        }

        if ftp.cwd(path).await.is_ok() {
            return Ok(());
        }

        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let is_abs = path.starts_with('/');
        
        if is_abs {
            let _ = ftp.cwd("/").await;
        }

        for part in parts {
            if ftp.cwd(part).await.is_err() {
                ftp.mkdir(part).await?;
                let chmod = self.config.perm_dir_default;
                if chmod != 0 {
                    let _ = ftp.site(format!("CHMOD {:o} {}", chmod, part)).await;
                }
                ftp.cwd(part).await?;
            }
        }

        Ok(())
    }

    fn get_accelerated(&self, msg: &Message, remote_file: &str, local_file: &Path) -> Option<u64> {
        let cmd_template = self.config.options.get("accelFtpgetCommand")?;
        let mut base_url = msg.base_url.clone();
        if base_url.ends_with('/') { base_url.pop(); }
        let url = format!("{}/{}", base_url, remote_file);
        let arg1 = url.replace(' ', "\\ ");
        let arg2 = local_file.to_string_lossy().to_string();
        let cmd_str = cmd_template.replace("%s", &arg1).replace("%d", &arg2);
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        if parts.is_empty() { return None; }
        log::info!("accel_ftp: {}", cmd_str);
        let status = Command::new(parts[0]).args(&parts[1..]).status().ok()?;
        if status.success() { std::fs::metadata(local_file).ok().map(|m| m.len()) } else { None }
    }

    fn put_accelerated(&self, msg: &Message, local_file: &Path, remote_file: &str) -> Option<u64> {
        let cmd_template = self.config.options.get("accelFtpputCommand")?;
        let target_url = self.config.send_to.as_ref().unwrap_or(&msg.base_url);
        let mut base_url = target_url.clone();
        if base_url.ends_with('/') { base_url.pop(); }
        let url = format!("{}/{}", base_url, remote_file);
        let arg1 = local_file.to_string_lossy().to_string();
        let arg2 = url.replace(' ', "\\ ");
        let cmd_str = cmd_template.replace("%s", &arg1).replace("%d", &arg2);
        let parts: Vec<&str> = cmd_str.split_whitespace().collect();
        if parts.is_empty() { return None; }
        log::info!("accel_ftp: {}", cmd_str);
        let status = Command::new(parts[0]).args(&parts[1..]).status().ok()?;
        if status.success() { msg.fields.get("size").and_then(|s| s.parse().ok()) } else { None }
    }
}

#[async_trait]
impl Transfer for FtpTransfer {
    async fn get(&self, msg: &Message, local_file: &Path) -> anyhow::Result<u64> {
        let rel_path = msg.rel_path.clone();
        if let Some(size) = self.get_accelerated(msg, &rel_path, local_file) {
            return Ok(size);
        }

        let mut ftp = self.connect(&msg.base_url).await?;
        
        if let Some(parent) = local_file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut local_f = tokio::fs::File::create(local_file).await?;
        
        let remote_offset = msg.fields.get("remote_offset").and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
        if remote_offset > 0 {
            ftp.resume_transfer(remote_offset).await?;
        }

        let mut reader = ftp.retr_as_stream(&rel_path).await?;
        let mut downloaded = 0;
        let mut buffer = [0; 16384];

        loop {
            let n = reader.read(&mut buffer).await?;
            if n == 0 { break; }
            local_f.write_all(&buffer[..n]).await?;
            downloaded += n as u64;
        }

        local_f.flush().await?;
        let _ = ftp.finalize_retr_stream(reader).await?;
        ftp.quit().await?;

        Ok(downloaded)
    }

    async fn put(&self, msg: &Message, local_file: &Path, remote_file: &str) -> anyhow::Result<u64> {
        if let Some(size) = self.put_accelerated(msg, local_file, remote_file) {
            return Ok(size);
        }

        let target_url = self.config.send_to.as_ref().unwrap_or(&msg.base_url);
        let mut ftp = self.connect(target_url).await?;

        if let Some(parent) = Path::new(remote_file).parent() {
            if let Some(p_str) = parent.to_str() {
                self.cd_forced(&mut ftp, p_str).await?;
            }
        }

        let filename = Path::new(remote_file).file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid remote filename"))?;

        let mut local_f = tokio::fs::File::open(local_file).await?;
        let _ = ftp.site("UMASK 000").await;

        ftp.put_file(filename, &mut local_f).await?;
        let uploaded = tokio::fs::metadata(local_file).await?.len();
        
        let chmod = self.config.perm_default;
        if chmod != 0 {
            let _ = ftp.site(format!("CHMOD {:o} {}", chmod, filename)).await;
        }

        ftp.quit().await?;
        Ok(uploaded)
    }

    async fn mkdir(&self, remote_dir: &str) -> anyhow::Result<()> {
        let target_url = if let Some(st) = &self.config.send_to {
            st.clone()
        } else {
            anyhow::bail!("mkdir requires sendTo configured for FTP");
        };

        let mut ftp = self.connect(&target_url).await?;

        // Ftp cd_forced creates the directory recursively if it doesn't exist
        self.cd_forced(&mut ftp, remote_dir).await?;

        let _ = ftp.quit().await;
        Ok(())
    }
}
