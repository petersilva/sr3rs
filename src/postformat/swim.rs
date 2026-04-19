use super::PostFormat;
use crate::Message;
use std::collections::HashMap;
use chrono::Utc;

pub struct Swim;

impl Swim {
    pub fn new() -> Self {
        Self
    }
}

impl PostFormat for Swim {
    fn name(&self) -> &'static str {
        "swim"
    }

    fn content_type(&self) -> &'static str {
        "unknown"
    }

    fn mine(
        &self,
        _payload: &[u8],
        headers: &HashMap<String, String>,
        _content_type: &str,
        _options: &serde_json::Value,
    ) -> bool {
        if let Some(conforms) = headers.get("conformsTo") {
            conforms.to_lowercase().contains("swim")
        } else {
            false
        }
    }

    fn import_mine(
        &self,
        body: &[u8],
        headers: &HashMap<String, String>,
        _options: &serde_json::Value,
    ) -> Option<Message> {
        let mut msg = Message::new("", "");
        msg.delete_on_post.insert("format".to_string(), "swim".to_string());
        msg.delete_on_post.insert("local_offset".to_string(), "0".to_string());
        
        if let Some(pubtime) = headers.get("properties.pubtime") {
            let clean_time = pubtime.replace("-", "").replace(":", "").replace("Z", ".00").replace("T", "");
            if let Some(dt) = Message::parse_v02_time(&clean_time) {
                msg.pub_time = dt;
            }
        } else {
            log::error!("SWIM message missing mandatory properties.pubtime");
        }

        let mut i = 0;
        loop {
            let rel_key = format!("links[{}].rel", i);
            if let Some(rel) = headers.get(&rel_key) {
                let rel_lower = rel.to_lowercase();
                if rel_lower == "canonical" || rel_lower == "update" {
                    let href_key = format!("links[{}].href", i);
                    let typ_key = format!("links[{}].type", i);
                    if let (Some(href), Some(typ)) = (headers.get(&href_key), headers.get(&typ_key)) {
                        let parts: Vec<&str> = href.split("://").collect();
                        if parts.len() == 2 {
                            let scheme = parts[0];
                            let rest = parts[1];
                            if let Some(slash_idx) = rest.find('/') {
                                let domain = &rest[..=slash_idx];
                                msg.rel_path = rest[slash_idx + 1..].to_string();
                                msg.base_url = format!("{}://{}", scheme, domain);
                                msg.fields.insert("contentType".to_string(), typ.clone());
                                break;
                            }
                        }
                    }
                }
            } else {
                break;
            }
            i += 1;
        }

        if let (Some(method), Some(val)) = (headers.get("properties.integrity.method"), headers.get("properties.integrity.value")) {
            msg.fields.insert("identity".to_string(), serde_json::json!({"method": method, "value": val}).to_string());
        }

        if !body.is_empty() {
            if let Some(ct) = headers.get("amqp1_content_type") {
                msg.fields.insert("contentType".to_string(), ct.clone());
            }

            let mut decoded_payload = String::new();
            if let Some(encoding) = headers.get("amqp1_content_encoding") {
                if encoding == "gzip" && body.len() >= 2 && body[0] == 0x1f && body[1] == 0x8b {
                    use std::io::Read;
                    use flate2::read::GzDecoder;
                    let mut d = GzDecoder::new(body);
                    if d.read_to_string(&mut decoded_payload).is_err() {
                        decoded_payload = String::from_utf8_lossy(body).into_owned();
                    }
                } else {
                    decoded_payload = String::from_utf8_lossy(body).into_owned();
                }
            } else {
                decoded_payload = String::from_utf8_lossy(body).into_owned();
            }

            msg.fields.insert("content".to_string(), serde_json::json!({"encoding": "utf-8", "value": decoded_payload}).to_string());
            msg.fields.insert("size".to_string(), decoded_payload.len().to_string());
        }

        if msg.base_url.is_empty() || msg.rel_path.is_empty() {
            msg.base_url = "http://fake/".to_string();
            if let Some(subj) = headers.get("amqp1_subject") {
                msg.rel_path = subj.clone();
            }
        }

        Some(msg)
    }

    fn export_mine(
        &self,
        msg: &Message,
        options: &serde_json::Value,
    ) -> Option<(String, HashMap<String, String>, String)> {
        let mut headers = HashMap::new();
        let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        
        let mut payload_bytes = Vec::new();
        let mut is_gzipped = false;

        if let Some(content_str) = msg.fields.get("content") {
            if let Ok(content_val) = serde_json::from_str::<serde_json::Value>(content_str) {
                if let Some(val_str) = content_val.get("value").and_then(|v| v.as_str()) {
                    for datatype in ["METAR", "TAF", "SIGMET", "SPECI"] {
                        if val_str.contains(&format!("iwxxm:{}", datatype)) {
                            if datatype == "METAR" || datatype == "SPECI" {
                                headers.insert("properties.datetime".to_string(), now.clone());
                                headers.insert("conformsTo".to_string(), "https://eur-registry.swim.aero/services/eurocontrol-iwxxm-metar-speci-subscription-and-request-service-10".to_string());
                                headers.insert("amqp1_default_priority".to_string(), if datatype == "SPECI" { "7".to_string() } else { "4".to_string() });
                            } else {
                                headers.insert("properties.end_datetime".to_string(), now.clone());
                                headers.insert("properties.start_datetime".to_string(), now.clone());
                                headers.insert("conformsTo".to_string(), format!("https://eur-registry.swim.aero/services/eurocontrol-iwxxm-{}-subscription-and-request-service-10", datatype.to_lowercase()));
                                headers.insert("amqp1_default_priority".to_string(), if datatype == "SIGMET" { "7".to_string() } else { "5".to_string() });
                            }

                            if let Some(topic_prefix) = options.get("post_topicPrefix").and_then(|t| t.as_array()) {
                                let prefix_str: Vec<String> = topic_prefix.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                                headers.insert("topic".to_string(), format!("{}.weather.{}", prefix_str.join("."), datatype.to_lowercase()));
                            } else {
                                headers.insert("topic".to_string(), "".to_string());
                            }
                            break;
                        }
                    }

                    // Attempt to find pubtime via basic XML search instead of full DOM parsing to save overhead
                    if let Some(idx) = val_str.find("<gml:timePosition") {
                        if let Some(end_idx) = val_str[idx..].find("</gml:timePosition>") {
                            if let Some(start_bracket) = val_str[idx..idx+end_idx].find('>') {
                                let pubtime = &val_str[idx + start_bracket + 1 .. idx + end_idx];
                                headers.insert("properties.pubtime".to_string(), pubtime.to_string());
                            }
                        }
                    }
                    
                    if !headers.contains_key("properties.pubtime") {
                        headers.insert("properties.pubtime".to_string(), now.clone());
                    }

                    let pt = headers.get("properties.pubtime").unwrap();
                    let (yyyy, mm, dd, hh, min, ss) = if pt.len() >= 19 {
                        (&pt[0..4], &pt[5..7], &pt[8..10], &pt[11..13], &pt[14..16], &pt[17..19])
                    } else {
                        ("0000", "00", "00", "00", "00", "00")
                    };
                    
                    headers.insert("amqp1_subject".to_string(), format!("DATA_NOTDEFINED_CWAO_NORMAL_{}{}{}{}{}{}", yyyy, mm, dd, hh, min, ss));

                    use std::io::Write;
                    use flate2::write::GzEncoder;
                    use flate2::Compression;
                    
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    if encoder.write_all(val_str.as_bytes()).is_ok() {
                        if let Ok(compressed) = encoder.finish() {
                            payload_bytes = compressed;
                            is_gzipped = true;
                            headers.insert("amqp1_content_encoding".to_string(), "gzip".to_string());
                        }
                    }
                    
                    if !is_gzipped {
                        headers.insert("amqp1_content_encoding".to_string(), "identity".to_string());
                        payload_bytes = val_str.as_bytes().to_vec();
                    }
                }
            }
        }

        if !msg.base_url.is_empty() && !msg.rel_path.is_empty() {
            headers.insert("links[0].type".to_string(), "application/xml".to_string());
            headers.insert("links[0].rel".to_string(), "canonical".to_string());
            headers.insert("links.count".to_string(), "1".to_string());
            
            let href = if msg.base_url.ends_with('/') || msg.rel_path.starts_with('/') {
                format!("{}{}", msg.base_url, msg.rel_path)
            } else {
                format!("{}/{}", msg.base_url, msg.rel_path)
            };
            headers.insert("links[0].href".to_string(), href);
        }

        if let Some(topic_prefix) = options.get("post_topicPrefix").and_then(|t| t.as_array()) {
            if !headers.contains_key("topic") {
                let prefix_str: Vec<String> = topic_prefix.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
                headers.insert("topic".to_string(), format!("{}.weather", prefix_str.join(".")));
            }
        }

        if let Some(topic) = headers.get("topic") {
            headers.insert("amqp1_address".to_string(), topic.clone());
        }

        if !headers.contains_key("amqp1_default_priority") {
            headers.insert("amqp1_default_priority".to_string(), "3".to_string());
        }

        if !headers.contains_key("properties.datetime") && !headers.contains_key("properties.start_datetime") {
            headers.insert("properties.datetime".to_string(), now.clone());
        }

        if !headers.contains_key("properties.pubtime") {
            headers.insert("properties.pubtime".to_string(), now.clone());
        }

        if let Some(identity_str) = msg.fields.get("identity") {
            if let Ok(identity_val) = serde_json::from_str::<serde_json::Value>(identity_str) {
                if let (Some(method), Some(val)) = (identity_val.get("method").and_then(|v| v.as_str()), identity_val.get("value").and_then(|v| v.as_str())) {
                    headers.insert("properties.integrity.method".to_string(), method.to_string());
                    headers.insert("properties.integrity.value".to_string(), val.to_string());
                }
            }
        }

        if let Some(ct) = msg.fields.get("contentType") {
            if ct.contains("xml") && msg.fields.contains_key("content") {
                headers.insert("amqp1_content_type".to_string(), "application/xml".to_string());
            } else {
                headers.insert("amqp1_content_type".to_string(), "application/uri-list".to_string());
            }
            if ct.contains("json") {
                headers.insert("amqp1_content_type".to_string(), "application/json".to_string());
            }
        }

        // Return base64 encoded string since we mapped the output to String instead of Vec<u8> globally across exports
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let body_out = if payload_bytes.is_empty() { "".to_string() } else { STANDARD.encode(payload_bytes) };

        Some((body_out, headers, self.content_type().to_string()))
    }
}
