//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message {
    pub base_url: String,
    pub rel_path: String,
    pub pub_time: chrono::DateTime<chrono::Utc>,
    #[serde(flatten)]
    pub fields: HashMap<String, String>,
    #[serde(skip)]
    pub ack_id: Option<String>,
}

impl Message {
    pub fn new(base_url: &str, rel_path: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            rel_path: rel_path.to_string(),
            pub_time: chrono::Utc::now(),
            fields: HashMap::new(),
            ack_id: None,
        }
    }

    pub fn from_file(path: &std::path::Path, config: &crate::Config) -> anyhow::Result<Self> {
        let abs_path = std::fs::canonicalize(path)?;
        let metadata = std::fs::metadata(&abs_path)?;
        
        let base_dir = config.post_base_dir.as_ref()
            .or(Some(&config.directory))
            .ok_or_else(|| anyhow::anyhow!("No directory or post_baseDir configured"))?;
        
        let abs_base_dir = std::fs::canonicalize(base_dir)?;
        
        let rel_path = abs_path.strip_prefix(&abs_base_dir)
            .map_err(|_| anyhow::anyhow!("File {} is not under base directory {}", abs_path.display(), abs_base_dir.display()))?;
        
        let base_url = config.post_base_url.as_deref()
            .unwrap_or("file://localhost/"); // Fallback

        let mut msg = Self::new(base_url, &rel_path.to_string_lossy());
        msg.fields.insert("size".to_string(), metadata.len().to_string());
        
        if let Ok(mtime) = metadata.modified() {
            let dt: chrono::DateTime<chrono::Utc> = mtime.into();
            msg.fields.insert("mtime".to_string(), dt.to_rfc3339());
        }

        Ok(msg)
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
}
