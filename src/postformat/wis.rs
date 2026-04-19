use super::PostFormat;
use crate::Message;
use std::collections::HashMap;

pub struct Wis;

impl Wis {
    pub fn new() -> Self {
        Self
    }
}

impl PostFormat for Wis {
    fn name(&self) -> &'static str {
        "wis"
    }

    fn content_type(&self) -> &'static str {
        "application/geo+json"
    }

    fn mine(
        &self,
        _payload: &[u8],
        _headers: &HashMap<String, String>,
        content_type: &str,
        _options: &serde_json::Value,
    ) -> bool {
        content_type == self.content_type()
    }

    fn import_mine(
        &self,
        _body: &[u8],
        _headers: &HashMap<String, String>,
        _options: &serde_json::Value,
    ) -> Option<Message> {
        let mut msg = Message::new("http://fake/", "");
        msg.delete_on_post.insert("format".to_string(), "wis".to_string());
        Some(msg)
    }

    fn export_mine(
        &self,
        _msg: &Message,
        _options: &serde_json::Value,
    ) -> Option<(String, HashMap<String, String>, String)> {
        Some(("{}".to_string(), HashMap::new(), self.content_type().to_string()))
    }
}
