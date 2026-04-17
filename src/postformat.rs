use crate::Message;
use std::collections::HashMap;

pub mod v02;
pub mod v03;
pub mod swim;
pub mod wis;

pub trait PostFormat: Send + Sync {
    fn name(&self) -> &'static str;
    fn content_type(&self) -> &'static str;
    
    fn mine(
        &self,
        payload: &[u8],
        headers: &HashMap<String, String>,
        content_type: &str,
        options: &serde_json::Value,
    ) -> bool;
    
    fn import_mine(
        &self,
        body: &[u8],
        headers: &HashMap<String, String>,
        options: &serde_json::Value,
    ) -> Option<Message>;
    
    fn export_mine(
        &self,
        msg: &Message,
        options: &serde_json::Value,
    ) -> Option<(String, HashMap<String, String>, String)>;
}

pub fn topic_derive(msg: &Message, options: &serde_json::Value) -> Vec<String> {
    // simplified topic derive matching python
    let mut topic = vec![];
    
    let mut topic_prefix = if let Some(publishers) = options.get("publishers").and_then(|p| p.as_array()) {
        if let Some(publisher_index) = options.get("publisher_index").and_then(|i| i.as_u64()) {
            if let Some(p) = publishers.get(publisher_index as usize) {
                if let Some(tp) = p.get("topicPrefix").or_else(|| p.get("topic_prefix")).and_then(|tp| tp.as_array()) {
                    tp.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect::<Vec<String>>()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    if topic_prefix.is_empty() {
        if let Some(tp) = options.get("topicPrefix").or_else(|| options.get("topic_prefix")).and_then(|tp| tp.as_array()) {
            topic_prefix = tp.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect::<Vec<String>>();
        }
    }

    if let Some(t) = msg.fields.get("topic") {
        topic = t.split('.').map(String::from).collect();
    } else {
        topic = topic_prefix;
        if !msg.rel_path.is_empty() {
            let parts: Vec<&str> = msg.rel_path.split('/').collect();
            if parts.len() > 1 {
                for part in &parts[0..parts.len() - 2] {
                    topic.push((*part).to_string());
                }
                ::log::error!(" normal topic derivation path: {:?}", topic );
            }
        } else if let Some(subtopic) = msg.delete_on_post.get("subtopic") {
            let parts: Vec<&str> = subtopic.split('.').collect();
            for part in parts {
                topic.push(part.to_string());
            }
        }
    }
    topic
}

pub fn import_any(
    payload: &[u8],
    headers: &HashMap<String, String>,
    content_type: &str,
    options: &serde_json::Value,
) -> Option<Message> {
    let formats: Vec<Box<dyn PostFormat>> = vec![
        Box::new(wis::Wis::new()),
        Box::new(swim::Swim::new()),
        Box::new(v03::V03::new()),
        Box::new(v02::V02::new()),
    ];
    for format in formats {
        if format.mine(payload, headers, content_type, options) {
            return format.import_mine(payload, headers, options);
        }
    }
    None
}

pub fn export_any(
    msg: &Message,
    post_format: &str,
    options: &serde_json::Value,
) -> Option<(String, HashMap<String, String>, String)> {
    let formats: Vec<Box<dyn PostFormat>> = vec![
        Box::new(wis::Wis::new()),
        Box::new(swim::Swim::new()),
        Box::new(v03::V03::new()),
        Box::new(v02::V02::new()),
    ];
    for format in formats {
        if format.name() == post_format {
            return format.export_mine(msg, options);
        }
    }
    None
}
