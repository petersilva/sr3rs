use super::{topic_derive, PostFormat};
use crate::Message;
use std::collections::HashMap;

pub struct V03;

impl V03 {
    pub fn new() -> Self {
        Self
    }
}

impl PostFormat for V03 {
    fn name(&self) -> &'static str {
        "v03"
    }

    fn content_type(&self) -> &'static str {
        "application/json"
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
        body: &[u8],
        _headers: &HashMap<String, String>,
        _options: &serde_json::Value,
    ) -> Option<Message> {
        let value: serde_json::Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(_) => return None,
        };

        let mut msg = Message::new("", "");
        msg.fields.insert("_format".to_string(), "v03".to_string());

        if let Some(obj) = value.as_object() {
            for (k, v) in obj {
                match k.as_str() {
                    "baseUrl" => msg.base_url = v.as_str().unwrap_or("").to_string(),
                    "relPath" => msg.rel_path = v.as_str().unwrap_or("").to_string(),
                    "pubTime" => {
                        if let Some(s) = v.as_str() {
                            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                                msg.pub_time = dt.with_timezone(&chrono::Utc);
                            }
                        }
                    }
                    "identity" => {
                        if let Some(obj) = v.as_object() {
                            for (id_k, id_v) in obj {
                                if let Some(s) = id_v.as_str() {
                                    msg.identity.insert(id_k.clone(), s.to_string());
                                } else {
                                    msg.identity.insert(id_k.clone(), id_v.to_string());
                                }
                            }
                        } else if let Some(s) = v.as_str() {
                            msg.fields.insert(k.clone(), s.to_string());
                        }
                    }
                    "fileOperation" | "file_operation" => {
                        if let Some(obj) = v.as_object() {
                            for (fo_k, fo_v) in obj {
                                if let Some(s) = fo_v.as_str() {
                                    msg.file_operation.insert(fo_k.clone(), s.to_string());
                                } else {
                                    msg.file_operation.insert(fo_k.clone(), fo_v.to_string());
                                }
                            }
                        }
                    }
                    "_deleteOnPost" | "delete_on_post" => {
                        // Ignore internal state fields
                    }
                    _ => {
                        if v.is_string() {
                            msg.fields.insert(k.clone(), v.as_str().unwrap().to_string());
                        } else {
                            msg.fields.insert(k.clone(), v.to_string());
                        }
                    }
                }
            }
        }

        if let Some(ret_path) = msg.fields.remove("retPath") {
            msg.fields.insert("retrievePath".to_string(), ret_path);
        }

        if let Some(integrity) = msg.fields.remove("integrity") {
            msg.fields.insert("identity".to_string(), integrity);
        }

        if let Some(parts) = msg.fields.remove("parts") {
            let p: Vec<&str> = parts.split(',').collect();
            if p.len() == 5 {
                if p[0] == "1" {
                    if let Ok(size) = p[1].parse::<u64>() {
                        msg.fields.insert("size".to_string(), size.to_string());
                    }
                } else {
                    let method = match p[0] {
                        "i" => "inplace",
                        "p" => "partitioned",
                        _ => p[0],
                    };
                    let blocks = serde_json::json!({
                        "method": method,
                        "size": p[1].parse::<u64>().unwrap_or(0),
                        "count": p[2].parse::<u64>().unwrap_or(0),
                        "remainder": p[3].parse::<u64>().unwrap_or(0),
                        "number": p[4].parse::<u64>().unwrap_or(0),
                    });
                    msg.fields.insert("blocks".to_string(), blocks.to_string());
                }
            }
        }

        if let Some(blocks_str) = msg.fields.get("blocks").cloned() {
            if let Ok(mut blocks_val) = serde_json::from_str::<serde_json::Value>(&blocks_str) {
                if let Some(blocks) = blocks_val.as_object_mut() {
                    if let Some(manifest) = blocks.get_mut("manifest") {
                        if let Some(m_obj) = manifest.as_object_mut() {
                            let mut new_manifest = serde_json::Map::new();
                            for (k, v) in m_obj.iter() {
                                new_manifest.insert(k.clone(), v.clone());
                            }
                            *manifest = serde_json::Value::Object(new_manifest);
                        }
                    }
                }
                msg.fields.insert("blocks".to_string(), blocks_val.to_string());
            }
        }

        Some(msg)
    }

    fn export_mine(
        &self,
        msg: &Message,
        options: &serde_json::Value,
    ) -> Option<(String, HashMap<String, String>, String)> {
        let mut body_map = serde_json::Map::new();
        body_map.insert("baseUrl".to_string(), serde_json::Value::String(msg.base_url.clone()));
        body_map.insert("relPath".to_string(), serde_json::Value::String(msg.rel_path.clone()));
        body_map.insert("pubTime".to_string(), serde_json::Value::String(msg.pub_time.to_rfc3339()));

        if !msg.identity.is_empty() {
            let mut identity_map = serde_json::Map::new();
            for (k, v) in &msg.identity {
                identity_map.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            body_map.insert("identity".to_string(), serde_json::Value::Object(identity_map));
        }

        if !msg.file_operation.is_empty() {
            let mut fo_map = serde_json::Map::new();
            for (k, v) in &msg.file_operation {
                fo_map.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            body_map.insert("fileOperation".to_string(), serde_json::Value::Object(fo_map));
        }

        for (k, v) in &msg.fields {
            if (k == "identity" && !msg.identity.is_empty()) ||
               (k == "fileOperation" && !msg.file_operation.is_empty()) ||
               (k == "file_operation" && !msg.file_operation.is_empty()) ||
               (k == "_deleteOnPost" || k == "delete_on_post") {
                continue;
            }
            if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(v) {
                body_map.insert(k.clone(), json_val);
            } else {
                body_map.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
        }

        let raw_body = serde_json::to_string(&body_map).ok()?;
        
        let mut headers = HashMap::new();
        let topic = topic_derive(msg, options).join(".");
        headers.insert("topic".to_string(), topic);

        Some((raw_body, headers, self.content_type().to_string()))
    }
}