use url::Url;
use crate::config::ConfigError;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Broker {
    #[serde(with = "url_serde")]
    pub url: Url,
    pub user: Option<String>,
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
        serializer.serialize_str(url.as_str())
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
        // Lapin/amq-protocol requires vhost to be encoded in the URI if it's the root vhost
        // but it actually handles amqp://user:pass@host/%2f correctly.
        // If vhost is "/", we should ensure it's represented as %2f in the path.
        let mut u = self.url.clone();
        if self.vhost == "/" {
            u.set_path("/%2f");
        } else {
            u.set_path(&format!("/{}", self.vhost));
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
        assert!(broker.to_lapin_uri().contains("/%2f"));
    }

    #[test]
    fn test_broker_parse_anonymous_default() {
        let broker = Broker::parse("amqps://dd.weather.gc.ca/").unwrap();
        assert_eq!(broker.user, Some("anonymous".to_string()));
        assert_eq!(broker.password, Some("anonymous".to_string()));
    }
}
