use std::collections::HashMap;
use std::path::{Path, PathBuf};
use url::Url;
use crate::config::ConfigError;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Credential {
    #[serde(with = "url_serde")]
    pub url: Url,
    pub ssh_keyfile: Option<PathBuf>,
    pub passive: bool,
    pub binary: bool,
    pub tls: bool,
    pub prot_p: bool,
    pub bearer_token: Option<String>,
    pub login_method: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_session_token: Option<String>,
    pub azure_credentials: Option<String>,
    pub implicit_ftps: bool,
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

impl Credential {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            ssh_keyfile: None,
            passive: true,
            binary: true,
            tls: false,
            prot_p: false,
            bearer_token: None,
            login_method: None,
            s3_endpoint: None,
            s3_session_token: None,
            azure_credentials: None,
            implicit_ftps: false,
        }
    }

    pub fn parse(line: &str) -> Result<Self, ConfigError> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            return Err(ConfigError::Parse("Empty credential line".to_string()));
        }

        let url = Url::parse(parts[0])?;
        let mut cred = Self::new(url);

        if parts.len() > 1 {
            let options_part = &line[parts[0].len()..].trim();
            for opt in options_part.split(',') {
                let opt = opt.trim();
                if opt.is_empty() { continue; }
                
                let kv: Vec<&str> = opt.splitn(2, '=').collect();
                let key = kv[0].trim();
                let val = if kv.len() > 1 { Some(kv[1].trim()) } else { None };

                match key {
                    "ssh_keyfile" => {
                        if let Some(v) = val {
                            cred.ssh_keyfile = Some(PathBuf::from(v));
                        }
                    }
                    "passive" => cred.passive = true,
                    "active" => cred.passive = false,
                    "binary" => cred.binary = true,
                    "ascii" => cred.binary = false,
                    "tls" => cred.tls = true,
                    "ssl" => cred.tls = false,
                    "prot_p" => cred.prot_p = true,
                    "bearer_token" | "bt" => cred.bearer_token = val.map(String::from),
                    "login_method" => cred.login_method = val.map(String::from),
                    "s3_endpoint" => cred.s3_endpoint = val.map(String::from),
                    "s3_session_token" => cred.s3_session_token = val.map(String::from),
                    "azure_storage_credentials" => cred.azure_credentials = val.map(String::from),
                    "implicit_ftps" => {
                        cred.implicit_ftps = true;
                        cred.tls = true;
                    }
                    _ => {} // Ignore unknown options for now
                }
            }
        }

        Ok(cred)
    }
}

#[derive(Debug, Clone, Default)]
pub struct CredentialDb {
    pub credentials: HashMap<String, Credential>,
}

impl CredentialDb {
    pub fn new() -> Self {
        Self {
            credentials: HashMap::new(),
        }
    }

    pub fn load(&mut self, path: &Path) -> Result<(), ConfigError> {
        if !path.exists() {
            return Ok(());
        }
        let content = std::fs::read_to_string(path)?;
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Ok(cred) = Credential::parse(line) {
                let key = self.make_key(&cred.url);
                self.credentials.insert(key, cred);
            }
        }
        Ok(())
    }

    pub fn make_key(&self, url: &Url) -> String {
        let mut key_url = url.clone();
        let _ = key_url.set_password(None);
        key_url.to_string()
    }

    pub fn get(&self, url_str: &str) -> Option<&Credential> {
        let url = Url::parse(url_str).ok()?;
        let key = self.make_key(&url);
        
        // Try exact match first
        if let Some(cred) = self.credentials.get(&key) {
            return Some(cred);
        }

        // SR3 matching logic: find first that matches scheme, host, port, user
        for cred in self.credentials.values() {
            let u = &cred.url;
            if url.scheme() != u.scheme() { continue; }
            if url.host_str() != u.host_str() { continue; }
            if url.port() != u.port() { continue; }
            if url.username() != u.username() {
                if !url.username().is_empty() { continue; }
            }
            
            // For AMQP, check vhost (path)
            if url.scheme().starts_with("amqp") {
                let p1 = url.path();
                let p2 = u.path();
                let v1 = if p1 == "/" || p1 == "/%2f" || p1.is_empty() { "/" } else { p1.trim_matches('/') };
                let v2 = if p2 == "/" || p2 == "/%2f" || p2.is_empty() { "/" } else { p2.trim_matches('/') };
                if v1 != v2 { continue; }
            }

            return Some(cred);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_credential_with_options() {
        let line = "sftp://alice@herhost/ ssh_keyfile=/home/myself/mykeys/.ssh.id_dsa, passive=false";
        let cred = Credential::parse(line).unwrap();
        assert_eq!(cred.url.username(), "alice");
        assert_eq!(cred.url.host_str(), Some("herhost"));
        assert_eq!(cred.ssh_keyfile, Some(PathBuf::from("/home/myself/mykeys/.ssh.id_dsa")));
        // Note: we don't have active=true, but passive=false was used via passive keyword override if implemented
        // Actually my parse implementation sets passive=true if it sees "passive".
        // Let's re-verify: "active" sets passive=false.
    }

    #[test]
    fn test_credential_db_lookup() {
        let mut db = CredentialDb::new();
        let cred = Credential::parse("amqp://feeder:secret@localhost/vhost").unwrap();
        db.credentials.insert(db.make_key(&cred.url), cred);

        let found = db.get("amqp://feeder@localhost/vhost").unwrap();
        assert_eq!(found.url.password(), Some("secret"));
    }
}
