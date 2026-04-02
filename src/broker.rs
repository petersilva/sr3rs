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
        let url = Url::parse(uri)?;
        let user = if !url.username().is_empty() {
            Some(url.username().to_string())
        } else {
            None
        };
        let password = url.password().map(|p| p.to_string());
        let host = url.host_str().map(|h| h.to_string());
        let port = url.port();

        Ok(Self {
            url,
            user,
            password,
            host,
            port,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_parse_amqp() {
        let broker = Broker::parse("amqp://feeder:pass@localhost:5672/").unwrap();
        assert_eq!(broker.user, Some("feeder".to_string()));
        assert_eq!(broker.password, Some("pass".to_string()));
        assert_eq!(broker.host, Some("localhost".to_string()));
        assert_eq!(broker.port, Some(5672));
    }

    #[test]
    fn test_broker_parse_mqtt() {
        let broker = Broker::parse("mqtt://broker.example.com/").unwrap();
        assert_eq!(broker.user, None);
        assert_eq!(broker.host, Some("broker.example.com".to_string()));
        assert_eq!(broker.port, None);
    }

    #[test]
    fn test_invalid_url() {
        let result = Broker::parse("not-a-url");
        assert!(result.is_err());
    }
}
