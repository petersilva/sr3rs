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
    pub ack_id: Option<u64>,
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
