use std::collections::HashMap;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub base_url: String,
    pub rel_path: String,
    pub pub_time: chrono::DateTime<chrono::Utc>,
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
