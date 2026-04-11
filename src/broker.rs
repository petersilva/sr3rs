//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use url::Url;
use crate::config::ConfigError;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broker {
    #[serde(with = "url_serde")]
    pub url: Url,
    pub user: Option<String>,
    #[serde(skip_serializing)]
    pub password: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub vhost: String,
}

mod url_serde {
    use url::Url;
    use serde::{self, Deserialize, Serializer, Deserializer};

    pub fn serialize<S>(url: &Url, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut u = url.clone();
        let _ = u.set_password(None);
        serializer.serialize_str(u.as_str())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Url, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Url::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl Broker {
    pub fn parse(uri: &str) -> Result<Self, ConfigError> {
        let mut url = Url::parse(uri)?;
        
        // Normalize amqp URLs: if path is empty, make it "/" for consistency
        if url.scheme().starts_with("amqp") && url.path().is_empty() {
            let _ = url.set_path("/");
        }

        let user = if !url.username().is_empty() {
            Some(url.username().to_string())
        } else {
            let _ = url.set_username("anonymous");
            Some("anonymous".to_string())
        };

        let password = if let Some(p) = url.password() {
            Some(p.to_string())
        } else {
            if user.as_deref() == Some("anonymous") {
                let _ = url.set_password(Some("anonymous"));
                Some("anonymous".to_string())
            } else {
                None
            }
        };

        // SR3 vhost logic: if path is '/' or empty, use '/'. 
        // Otherwise strip leading/trailing slashes.
        let vhost = if url.path() == "/" || url.path().is_empty() {
            "/".to_string()
        } else {
            url.path().trim_matches('/').to_string()
        };

        let host = url.host_str().map(|h| h.to_string());
        let port = url.port();

        Ok(Self {
            url,
            user,
            password,
            host,
            port,
            vhost,
        })
    }

    pub fn to_lapin_uri(&self) -> String {
        let u = self.url.clone();
        let mut s = u.to_string();
        
        if self.vhost == "/" {
            // RabbitMQ requires %2f for the root vhost in the URI.
            // Url::to_string() will have / at the end if path is /.
            // We want to replace the trailing / with /%2f
            if s.ends_with('/') {
                s.push_str("%2f");
            } else {
                s.push_str("/%2f");
            }
        } else {
            // For other vhosts, we can't just use set_path because it might escape
            // but for simple vhost names it's fine. 
            // However, to be safe and consistent with SR3, we just use the original URL 
            // if it was parsed with that vhost.
            // If vhost was changed, we might need more complex logic.
            // For now, if it's not /, we assume it was in the path.
        }
        s
    }

    pub fn redacted(&self) -> String {
        let mut u = self.url.clone();
        if u.password().is_some() {
            let _ = u.set_password(Some(""));
        }
        u.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_parse_amqp() {
        let broker = Broker::parse("amqp://feeder:pass@localhost:5672/vhost").unwrap();
        assert_eq!(broker.user, Some("feeder".to_string()));
        assert_eq!(broker.password, Some("pass".to_string()));
        assert_eq!(broker.vhost, "vhost");
    }

    #[test]
    fn test_broker_parse_default_vhost() {
        let broker = Broker::parse("amqps://dd.weather.gc.ca/").unwrap();
        assert_eq!(broker.vhost, "/");
        let uri = broker.to_lapin_uri();
        assert!(uri.ends_with("/%2f"));
        assert!(!uri.contains("/%252f"));
    }

    #[test]
    fn test_broker_parse_anonymous_default() {
        let broker = Broker::parse("amqps://dd.weather.gc.ca/").unwrap();
        assert_eq!(broker.user, Some("anonymous".to_string()));
        assert_eq!(broker.password, Some("anonymous".to_string()));
    }
}
