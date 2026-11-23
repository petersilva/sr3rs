//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Report {
    pub code: u16,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub base_url: String,
    pub rel_path: String,
    pub pub_time: chrono::DateTime<chrono::Utc>,
    #[serde(flatten)]
    pub fields: HashMap<String, String>,

    pub identity: HashMap<String, String>,

    pub file_operation: HashMap<String, String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub report: Option<Report>,

    #[serde(flatten)]
    pub delete_on_post: HashMap<String, String>,

    #[serde(skip)]
    pub ack_id: Option<String>,
}

impl Message {
    pub fn new(base_url: &str, rel_path: &str) -> Self {
        let mut b = base_url.to_string();
        if !b.ends_with('/') {
            b.push('/');
        }
        let r = rel_path.trim_start_matches('/').to_string();
        Self {
            base_url: b,
            rel_path: r,
            pub_time: chrono::Utc::now(),
            fields: HashMap::new(),
            identity: HashMap::new(),
            file_operation: HashMap::new(),
            report: None,
            delete_on_post: HashMap::new(),
            ack_id: None,
        }
    }

    pub fn set_report(&mut self, code: u16, message: impl Into<String>) {
        let msg_str = message.into();
        self.delete_on_post.insert("reject".to_string(), msg_str.clone());
        self.report = Some(Report {
            code,
            message: msg_str,
        });
    }

    pub fn get_id_str(&self) -> String {
        let mut s = String::new();
        if !self.base_url.is_empty() {
            s.push_str(&self.base_url);
        } else {
            s.push_str("baseUrl missing ");
        }
        if !self.rel_path.is_empty() {
            if !self.rel_path.starts_with('/') && !s.is_empty() && !s.ends_with('/') {
                s.push('/');
            }
            s.push_str(&self.rel_path);
        } else if let Some(ret_path) = self.fields.get("retrievePath") {
            if !ret_path.starts_with('/') && !s.is_empty() && !s.ends_with('/') {
                s.push('/');
            }
            s.push_str(ret_path);
        } else {
            s.push_str("badMessage");
        }
        s
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_file(path: &std::path::Path, config: &crate::Config, base_dir: &std::path::Path) -> anyhow::Result<Self> {
        let abs_path = std::fs::canonicalize(path).map_err(|e| anyhow::anyhow!("Failed to canonicalize path {:?}: {}", path, e))?;
        let metadata = std::fs::metadata(&abs_path)?;

        /*
        let base_dir = config.post_base_dir.as_ref()
            .or(Some(&config.directory))
            .ok_or_else(|| anyhow::anyhow!("No directory or post_baseDir configured"))?;

        let mut vars = std::collections::HashMap::new();
        for (k, v) in std::env::vars() {
            vars.insert(k, v);
        }
        let expanded_base_dir = crate::config::variable_expansion::expand_variables(
            &base_dir.to_string_lossy(),
            &vars
        );

        let abs_base_dir = std::fs::canonicalize(&expanded_base_dir).unwrap_or_else(|_| std::path::PathBuf::from(&expanded_base_dir));
         */

        let rel_path = abs_path.strip_prefix(&base_dir)
            .unwrap_or_else(|_| abs_path.as_path());
        
        /*
        let base_url = config.post_base_url.as_deref()
            .unwrap_or("file://localhost/"); // Fallback
         */
        let base_url = format!("file:{}", base_dir.to_string_lossy() ) ;

        let mut msg = Self::new( &base_url, &rel_path.to_string_lossy());
        msg.fields.insert("size".to_string(), metadata.len().to_string());

        // identity if it is a file, fileOp if not.
        if metadata.is_file() {
            let mut id =  crate::identity::factory( &config.identity_method ).unwrap();
            let _ = id.update_file( &abs_path.to_str().unwrap() );
            msg.identity.insert( "method".to_string(), id.get_method() );
            msg.identity.insert( "value".to_string(), id.value() );
        } else if metadata.is_dir() {
            msg.file_operation.insert("directory".to_string(), "".to_string() );
        } else if metadata.is_symlink() {
            let destination = fs::read_link(&abs_path).unwrap() ;
            msg.file_operation.insert("link".to_string(), destination.display().to_string() );
        } // FIXME: missing special files.

        // FIXME: missing username, groupname, mode

        if let Ok(mtime) = metadata.modified() {
            let dt: chrono::DateTime<chrono::Utc> = mtime.into();
            msg.fields.insert("mtime".to_string(), dt.to_rfc3339());
        }
        if let Ok(mtime) = metadata.accessed() {
            let dt: chrono::DateTime<chrono::Utc> = mtime.into();
            msg.fields.insert("atime".to_string(), dt.to_rfc3339());
        }
        log::warn!( "message::from_file Message {:?}", &msg );

        Ok(msg)
    }

    pub fn put_content_inline(&mut self, path: &std::path::Path, inline_byte_max: u64) {
        if self.fields.contains_key("content") {
            return;
        }

        if let Ok(metadata) = std::fs::metadata(path) {
            let size = metadata.len();
            if size >= inline_byte_max {
                log::warn!(
                    "Not placing file contents in message due to file size being too big. File size {}, inlineByteMax: {}",
                    size,
                    inline_byte_max
                );
                return;
            }

            if let Ok(bytes) = std::fs::read(path) {
                let content_obj = match String::from_utf8(bytes.clone()) {
                    Ok(text) => serde_json::json!({
                        "value": text,
                        "encoding": "utf-8"
                    }),
                    Err(_) => {
                        use base64::{Engine as _, engine::general_purpose::STANDARD};
                        serde_json::json!({
                            "value": STANDARD.encode(&bytes),
                            "encoding": "base64"
                        })
                    }
                };
                self.fields.insert("content".to_string(), content_obj.to_string());
            }
        }
    }

    pub fn parse_v02_time(s: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        // v02 time is usually YYYYMMDDHHMMSS.sss
        if s.len() < 14 { return None; }
        let year = s[0..4].parse().ok()?;
        let month = s[4..6].parse().ok()?;
        let day = s[6..8].parse().ok()?;
        let hour = s[8..10].parse().ok()?;
        let min = s[10..12].parse().ok()?;
        let sec = s[12..14].parse().ok()?;
        let milli = if s.len() > 15 && s.get(14..15) == Some(".") {
            s[15..].parse::<u32>().ok().unwrap_or(0)
        } else {
            0
        };

        use chrono::TimeZone;
        chrono::Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
            .single()
            .map(|dt| dt + chrono::Duration::milliseconds(milli as i64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

#[test]
fn test_message_creation() {
    let msg = Message::new("sftp://user@host/", "path/to/file.txt");
    assert_eq!(msg.base_url, "sftp://user@host/");
    assert_eq!(msg.rel_path, "path/to/file.txt");
    assert!(msg.fields.is_empty());
}

#[test]
fn test_message_fields() {
    let mut msg = Message::new("http://host/", "file.txt");
    msg.fields.insert("product".to_string(), "METEO".to_string());
    assert_eq!(msg.fields.get("product").unwrap(), "METEO");
}

#[test]
fn test_put_content_inline() {
    use tempfile::NamedTempFile;
    use std::io::Write;

    let mut file = NamedTempFile::new().unwrap();
    write!(file, "hello inline world").unwrap();

    let mut msg = Message::new("file:/", "test.txt");
    msg.put_content_inline(file.path(), 4096);

    let content_str = msg.fields.get("content").unwrap();
    assert!(content_str.contains("hello inline world"));
    assert!(content_str.contains("utf-8"));

    // Test file too big
    let mut msg2 = Message::new("file:/", "test.txt");
    msg2.put_content_inline(file.path(), 5); // max size 5 < 18
    assert!(!msg2.fields.contains_key("content"));
}
}
