use super::{topic_derive, PostFormat};
use crate::Message;
use std::collections::HashMap;

pub struct V02;

impl V02 {
    pub fn new() -> Self {
        Self
    }
}

impl PostFormat for V02 {
    fn name(&self) -> &'static str {
        "v02"
    }

    fn content_type(&self) -> &'static str {
        "text/plain"
    }

    fn mine(
        &self,
        payload: &[u8],
        headers: &HashMap<String, String>,
        content_type: &str,
        _options: &serde_json::Value,
    ) -> bool {
        if content_type == self.content_type() {
            return true;
        }

        if let Ok(s) = std::str::from_utf8(payload) {
            if s.len() >= 5 && !s[0..5].contains('{') {
                return true;
            }
        }

        if let Some(topic) = headers.get("topic") {
            if topic.starts_with("v02.") {
                return true;
            }
        }

        false
    }

    fn import_mine(
        &self,
        body: &[u8],
        headers: &HashMap<String, String>,
        _options: &serde_json::Value,
    ) -> Option<Message> {
        let body_str = std::str::from_utf8(body).ok()?;
        let parts: Vec<&str> = body_str.split(' ').collect();
        if parts.len() < 3 {
            return None;
        }

        let pub_time_str = parts[0];
        let base_url = parts[1].replace("%20", " ").replace("%23", "#");
        let rel_path = parts[2];

        let mut msg = Message::new(&base_url, rel_path);
        msg.fields.insert("_format".to_string(), "v02".to_string());
        
        for (k, v) in headers {
            msg.fields.insert(k.clone(), v.clone());
        }

        if let Some(dt) = Message::parse_v02_time(pub_time_str) {
            msg.pub_time = dt;
        }

        msg.fields.insert("subtopic".to_string(), serde_json::to_string(&rel_path.split('/').collect::<Vec<&str>>()).unwrap_or_default());
        msg.fields.insert("to_clusters".to_string(), "ALL".to_string());

        if let Some(integrity) = msg.fields.remove("integrity") {
            msg.fields.insert("identity".to_string(), integrity);
        }

        let delete_on_post = vec!["subtopic"];
        msg.fields.insert("_deleteOnPost".to_string(), serde_json::to_string(&delete_on_post).unwrap());

        for t in ["atime", "mtime"] {
            if let Some(time_val) = msg.fields.get(t).cloned() {
                if let Some(dt) = Message::parse_v02_time(&time_val) {
                    msg.fields.insert(t.to_string(), dt.to_rfc3339());
                }
            }
        }

        if let Some(sum_val) = msg.fields.remove("sum") {
            if sum_val.len() > 0 {
                let algo_char = &sum_val[0..1];
                let sv = if sum_val.len() > 2 { &sum_val[2..] } else { "" };
                let mut sm = "";
                let mut decode_sv = false;
                
                match algo_char {
                    "0" => { sm = "random"; },
                    "a" => { sm = "arbitrary"; },
                    "d" => { sm = "md5"; decode_sv = true; },
                    "L" => { sm = "link"; decode_sv = true; },
                    "m" => { sm = "mkdir"; decode_sv = true; },
                    "n" => { sm = "md5name"; decode_sv = true; },
                    "r" => { sm = "rmdir"; decode_sv = true; },
                    "R" => { sm = "remove"; decode_sv = true; },
                    "s" => { sm = "sha512"; decode_sv = true; },
                    "z" => { sm = "cod"; },
                    _ => {}
                }

                let final_sv = if decode_sv {
                    // Python v02 does hex decode then base64 encode
                    if let Ok(hex_bytes) = hex::decode(sv) {
                        use base64::{Engine as _, engine::general_purpose::STANDARD};
                        STANDARD.encode(hex_bytes)
                    } else {
                        sv.to_string()
                    }
                } else {
                    sv.to_string()
                };

                if let Some(oldname) = msg.fields.remove("oldname") {
                    let mut file_op = serde_json::Map::new();
                    file_op.insert("rename".to_string(), serde_json::Value::String(oldname));
                    match sm {
                        "mkdir" => { file_op.insert("mkdir".to_string(), serde_json::Value::String("".to_string())); },
                        "link" => {
                            if let Some(link_val) = msg.fields.remove("link") {
                                file_op.insert("link".to_string(), serde_json::Value::String(link_val));
                            }
                        },
                        _ => {
                            msg.fields.insert("identity".to_string(), serde_json::json!({"method": sm, "value": final_sv}).to_string());
                        }
                    }
                    msg.fields.insert("fileOp".to_string(), serde_json::to_string(&file_op).unwrap());
                } else if sm == "remove" {
                    msg.fields.insert("fileOp".to_string(), serde_json::json!({"remove": ""}).to_string());
                } else if sm == "mkdir" {
                    msg.fields.insert("fileOp".to_string(), serde_json::json!({"directory": ""}).to_string());
                } else if sm == "rmdir" {
                    msg.fields.insert("fileOp".to_string(), serde_json::json!({"remove": "", "directory": ""}).to_string());
                } else if let Some(link) = msg.fields.remove("link") {
                    msg.fields.insert("fileOp".to_string(), serde_json::json!({"link": link}).to_string());
                } else if sm == "md5name" {
                    // pass
                } else if sm != "" {
                    msg.fields.insert("identity".to_string(), serde_json::json!({"method": sm, "value": final_sv}).to_string());
                }
            }
        }

        if let Some(parts) = msg.fields.remove("parts") {
            let p: Vec<&str> = parts.split(',').collect();
            if p.len() >= 2 {
                let style = p[0];
                let chunksz = p[1];
                if style != "i" && style != "p" {
                    if let Ok(sz) = chunksz.parse::<u64>() {
                        msg.fields.insert("size".to_string(), sz.to_string());
                    }
                }
            }
        }

        Some(msg)
    }

    fn export_mine(
        &self,
        msg: &Message,
        options: &serde_json::Value,
    ) -> Option<(String, HashMap<String, String>, String)> {
        // v02 wrapper is tricky to do fully since we don't have the python v2wrapper logic.
        // We will output a space separated string and format headers to match v2 semantics as best we can.
        
        let pub_time = msg.pub_time.format("%Y%m%d%H%M%S.000").to_string(); // rough approx to timev2 format
        let raw_body = format!("{} {} {}", pub_time, msg.base_url, msg.rel_path);
        
        let mut headers = HashMap::new();
        for (k, v) in &msg.fields {
            if !["pubTime", "baseUrl", "fileOp", "relPath", "size", "blocks", "content", "identity", "publisher_index"].contains(&k.as_str()) {
                headers.insert(k.clone(), v.clone());
            }
        }
        
        headers.insert("topic".to_string(), topic_derive(msg, options).join("."));

        Some((raw_body, headers, self.content_type().to_string()))
    }
}
