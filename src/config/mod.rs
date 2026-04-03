use std::path::{Path, PathBuf};
use std::collections::HashMap;
use crate::filter::Filter;
use crate::broker::Broker;
use thiserror::Error;
use serde::{Serialize, Deserialize};

pub mod variable_expansion;
pub mod credentials;
pub mod subscription;
pub mod publisher;
pub mod paths;

use credentials::{CredentialDb, Credential};
use subscription::{Subscription, Binding};
use publisher::Publisher;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Parse error in {file}:{line}: {message}")]
    ParseContext {
        file: String,
        line: usize,
        message: String,
    },
    #[error("Parse error: {0}")]
    Parse(String),
    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),
    #[error("Url error: {0}")]
    Url(#[from] url::ParseError),
    #[error("File not found: {0}")]
    FileNotFound(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub appname: String,
    pub component: String,
    pub configname: Option<String>,
    pub broker: Option<Broker>,
    pub exchange: String,
    pub queue_name: String,
    pub prefetch: u32,
    pub subtopics: Vec<String>,
    pub topic_prefix: Vec<String>,
    pub masks: Vec<Filter>,
    pub accept_unmatched: bool,
    pub directory: PathBuf,
    pub download: bool,
    pub mirror: bool,
    pub instances: u32,
    pub housekeeping: u32, // seconds
    pub log_level: String,
    
    #[serde(skip)]
    pub credentials: CredentialDb,
    pub subscriptions: Vec<Subscription>,
    pub publishers: Vec<Publisher>,
    
    // Post (publishing) options
    pub post_broker: Option<Broker>,
    pub post_exchange: Option<String>,
    pub post_exchange_suffix: Option<String>,
    pub post_exchange_split: u32,
    pub post_topic_prefix: Option<Vec<String>>,
    pub post_format: Option<String>,
    pub post_base_dir: Option<PathBuf>,
    pub post_base_url: Option<String>,

    // Advanced options from default_options
    pub attempts: u32,
    pub batch: u32,
    pub delete: bool,
    pub dry_run: bool,
    pub nodupe_ttl: u32,
    pub overwrite: bool,
    pub recursive: bool,
    pub timeout: u32,
    pub sleep: f64,
    pub perm_default: u32,
    pub perm_dir_default: u32,

    pub options: HashMap<String, String>,
    pub config_search_paths: Vec<PathBuf>,
    pub rand4: String,
    pub rand8: String,
}

impl Default for Config {
    fn default() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let r4: u16 = rng.gen_range(0..0xFFFF);
        let r8: u32 = rng.gen();

        Self {
            appname: "sr3".to_string(),
            component: "flow".to_string(),
            configname: None,
            broker: None,
            exchange: "xpublic".to_string(),
            queue_name: "q_${BROKER_USER}.${COMPONENT}.${CONFIG}.${QUEUESHARE}".to_string(),
            prefetch: 10,
            subtopics: Vec::new(),
            topic_prefix: vec!["v02".to_string(), "post".to_string()],
            masks: Vec::new(),
            accept_unmatched: true,
            directory: PathBuf::from("."),
            download: false,
            mirror: true, // Default for most components in SR3
            instances: 1,
            housekeeping: 300,
            log_level: "info".to_string(),
            credentials: CredentialDb::new(),
            subscriptions: Vec::new(),
            publishers: Vec::new(),

            post_broker: None,
            post_exchange: None,
            post_exchange_suffix: None,
            post_exchange_split: 1,
            post_topic_prefix: None,
            post_format: None,
            post_base_dir: None,
            post_base_url: None,

            attempts: 3,
            batch: 100,
            delete: false,
            dry_run: false,
            nodupe_ttl: 0,
            overwrite: true,
            recursive: true,
            timeout: 300,
            sleep: 0.1,
            perm_default: 0,
            perm_dir_default: 0o775,

            options: HashMap::new(),
            config_search_paths: vec![PathBuf::from(".")],
            rand4: format!("{:04x}", r4),
            rand8: format!("{:08x}", r8),
        }
    }
}

impl Config {
    pub fn new() -> Self {
        let mut cfg = Self::default();
        let config_dir = paths::get_user_config_dir();
        
        // Load admin.conf then default.conf as per SR3
        for g in ["admin.conf", "default.conf"] {
            let path = config_dir.join(g);
            if path.exists() {
                if let Err(e) = cfg.read_file(path.to_str().unwrap()) {
                    log::warn!("Failed to load global config {}: {}", path.display(), e);
                }
            }
        }
        cfg
    }

    pub fn load(&mut self, input_path: &str) -> Result<(), ConfigError> {
        let path = self.resolve_config_path(input_path)?;
        
        // configname should be the stem (filename without extension)
        if let Some(stem) = path.file_stem() {
            self.configname = Some(stem.to_string_lossy().to_string());
        }

        // Add the directory of the config file to search paths for includes
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                self.config_search_paths.insert(0, parent.to_path_buf());
            }
        }
        self.read_file(path.to_str().unwrap())
    }

    fn resolve_config_path(&self, input: &str) -> Result<PathBuf, ConfigError> {
        let mut try_paths = Vec::new();
        
        try_paths.push(PathBuf::from(input));
        try_paths.push(PathBuf::from(format!("{}.conf", input)));
        
        let config_dir = paths::get_user_config_dir();
        try_paths.push(config_dir.join(input));
        try_paths.push(config_dir.join(format!("{}.conf", input)));

        try_paths.push(config_dir.join(self.component.clone()).join(input));
        try_paths.push(config_dir.join(self.component.clone()).join(format!("{}.conf", input)));

        for p in try_paths {
            if p.exists() && p.is_file() {
                return Ok(p);
            }
        }

        Err(ConfigError::FileNotFound(input.to_string()))
    }

    fn read_file(&mut self, path: &str) -> Result<(), ConfigError> {
        let content = std::fs::read_to_string(path).map_err(|_| ConfigError::FileNotFound(path.to_string()))?;
        self.parse_string(&content, path)
    }

    pub fn apply_component_defaults(&mut self, component: &str) {
        self.component = component.to_string();
        match component {
            "poll" => {
                self.nodupe_ttl = 7 * 3600;
                self.perm_default = 0o400;
                self.sleep = 5.0;
                self.mirror = true;
            }
            "subscribe" => {
                self.download = true;
                self.mirror = false;
            }
            "post" | "watch" => {
                self.download = false;
                self.mirror = true;
                self.sleep = 5.0;
            }
            "shovel" => {
                self.download = false;
                self.mirror = false;
            }
            "winnow" => {
                self.nodupe_ttl = 300;
                self.mirror = true;
            }
            _ => {}
        }
    }

    pub fn parse_string(&mut self, content: &str, filename: &str) -> Result<(), ConfigError> {
        let mut vars = self.options.clone();
        vars.insert("APPNAME".to_string(), self.appname.clone());
        vars.insert("COMPONENT".to_string(), self.component.clone());
        if let Some(cn) = &self.configname {
            vars.insert("CONFIG".to_string(), cn.clone());
        }
        vars.insert("RAND4".to_string(), self.rand4.clone());
        vars.insert("RAND8".to_string(), self.rand8.clone());
        vars.insert("USER".to_string(), std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()));
        vars.insert("HOSTNAME".to_string(), "localhost".to_string());

        for (line_no, line) in content.lines().enumerate() {
            let line_no = line_no + 1;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let expanded_line = variable_expansion::expand_variables(line, &vars);
            if expanded_line != line {
                log::debug!("CONFIG: expanded '{}' to '{}'", line, expanded_line);
            }

            let parts: Vec<&str> = expanded_line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            let k = parts[0];
            let v = if parts.len() > 1 {
                let value_raw = line[parts[0].len()..].trim();
                let value_expanded = variable_expansion::expand_variables(value_raw, &vars);
                Some(value_expanded)
            } else {
                None
            };

            if k == "declare" && parts.len() > 2 && parts[1] == "env" {
                let env_part = parts[2];
                let kv: Vec<&str> = env_part.splitn(2, '=').collect();
                if kv.len() == 2 {
                    let env_k = kv[0];
                    let env_v = variable_expansion::expand_variables(kv[1], &vars);
                    std::env::set_var(env_k, &env_v);
                    vars.insert(env_k.to_string(), env_v);
                    log::debug!("CONFIG: declared env {}={}", env_k, vars.get(env_k).unwrap());
                }
                continue;
            }

            let result = match k {
                "broker" => {
                    if let Some(ref val) = v {
                        self.broker = Some(Broker::parse(val).map_err(|e| ConfigError::ParseContext {
                            file: filename.to_string(),
                            line: line_no,
                            message: format!("Broker error: {} (raw: {})", e, val),
                        })?);
                    }
                    Ok(())
                }
                "exchange" => {
                    if let Some(ref val) = v {
                        self.exchange = val.to_string();
                    }
                    Ok(())
                }
                "queueName" => {
                    if let Some(ref val) = v {
                        self.queue_name = val.to_string();
                    }
                    Ok(())
                }
                "subtopic" | "topic" => {
                    if let Some(ref val) = v {
                        self.parse_binding(val, k == "topic").map_err(|e| ConfigError::ParseContext {
                            file: filename.to_string(),
                            line: line_no,
                            message: format!("Binding error: {}", e),
                        })?;
                    }
                    Ok(())
                }
                "topicPrefix" => {
                    if let Some(ref val) = v {
                        self.topic_prefix = val.split('.').map(|s| s.to_string()).collect();
                    }
                    Ok(())
                }
                "accept" => {
                    if let Some(ref val) = v {
                        self.masks.push(Filter::new(val, true, self.directory.clone(), self.mirror).map_err(|e| ConfigError::ParseContext {
                            file: filename.to_string(),
                            line: line_no,
                            message: format!("Accept error: {}", e),
                        })?);
                    }
                    Ok(())
                }
                "reject" => {
                    if let Some(ref val) = v {
                        self.masks.push(Filter::new(val, false, self.directory.clone(), self.mirror).map_err(|e| ConfigError::ParseContext {
                            file: filename.to_string(),
                            line: line_no,
                            message: format!("Reject error: {}", e),
                        })?);
                    }
                    Ok(())
                }
                "directory" => {
                    if let Some(ref val) = v {
                        self.directory = PathBuf::from(val);
                    }
                    Ok(())
                }
                "acceptUnmatched" => {
                    if let Some(ref val) = v {
                        self.accept_unmatched = is_true(val);
                    }
                    Ok(())
                }
                "prefetch" => {
                    if let Some(ref val) = v {
                        self.prefetch = parse_count(val);
                    }
                    Ok(())
                }
                "download" => {
                    if let Some(ref val) = v {
                        self.download = is_true(val);
                    }
                    Ok(())
                }
                "mirror" => {
                    if let Some(ref val) = v {
                        self.mirror = is_true(val);
                    }
                    Ok(())
                }
                "instances" => {
                    if let Some(ref val) = v {
                        self.instances = parse_count(val);
                    }
                    Ok(())
                }
                "housekeeping" => {
                    if let Some(ref val) = v {
                        self.housekeeping = parse_duration(val);
                    }
                    Ok(())
                }
                "logLevel" => {
                    if let Some(ref val) = v {
                        self.log_level = val.to_string();
                    }
                    Ok(())
                }
                "include" | "config" => {
                    if let Some(ref val) = v {
                        self.include_file(val)?;
                    }
                    Ok(())
                }

                "post_broker" => {
                    if let Some(ref val) = v {
                        if self.post_broker.is_some() {
                            self.parse_publisher();
                        }
                        self.post_broker = Some(Broker::parse(val).map_err(|e| ConfigError::ParseContext {
                            file: filename.to_string(),
                            line: line_no,
                            message: format!("post_broker error: {} (raw: {})", e, val),
                        })?);
                    }
                    Ok(())
                }
                "post_exchange" => {
                    if let Some(ref val) = v {
                        self.post_exchange = Some(val.to_string());
                    }
                    Ok(())
                }
                "post_exchangeSuffix" => {
                    if let Some(ref val) = v {
                        self.post_exchange_suffix = Some(val.to_string());
                    }
                    Ok(())
                }
                "post_exchangeSplit" => {
                    if let Some(ref val) = v {
                        self.post_exchange_split = parse_count(val);
                    }
                    Ok(())
                }
                "post_topicPrefix" => {
                    if let Some(ref val) = v {
                        self.post_topic_prefix = Some(val.split('.').map(|s| s.to_string()).collect());
                    }
                    Ok(())
                }
                "post_format" => {
                    if let Some(ref val) = v {
                        self.post_format = Some(val.to_string());
                    }
                    Ok(())
                }
                "post_baseDir" => {
                    if let Some(ref val) = v {
                        self.post_base_dir = Some(PathBuf::from(val));
                    }
                    Ok(())
                }
                "post_baseUrl" => {
                    if let Some(ref val) = v {
                        self.post_base_url = Some(val.to_string());
                    }
                    Ok(())
                }

                "attempts" => {
                    if let Some(ref val) = v {
                        self.attempts = parse_count(val);
                    }
                    Ok(())
                }
                "batch" => {
                    if let Some(ref val) = v {
                        self.batch = parse_count(val);
                    }
                    Ok(())
                }
                "delete" => {
                    if let Some(ref val) = v {
                        self.delete = is_true(val);
                    }
                    Ok(())
                }
                "dry_run" | "simulate" => {
                    if let Some(ref val) = v {
                        self.dry_run = is_true(val);
                    }
                    Ok(())
                }
                "nodupe_ttl" | "caching" => {
                    if let Some(ref val) = v {
                        self.nodupe_ttl = parse_duration(val);
                    }
                    Ok(())
                }
                "overwrite" => {
                    if let Some(ref val) = v {
                        self.overwrite = is_true(val);
                    }
                    Ok(())
                }
                "recursive" => {
                    if let Some(ref val) = v {
                        self.recursive = is_true(val);
                    }
                    Ok(())
                }
                "timeout" => {
                    if let Some(ref val) = v {
                        self.timeout = parse_duration(val);
                    }
                    Ok(())
                }
                "sleep" => {
                    if let Some(ref val) = v {
                        self.sleep = parse_duration(val) as f64;
                    }
                    Ok(())
                }
                "permDefault" | "chmod" => {
                    if let Some(ref val) = v {
                        self.perm_default = parse_octal(val);
                    }
                    Ok(())
                }
                "permDirDefault" | "chmod_dir" => {
                    if let Some(ref val) = v {
                        self.perm_dir_default = parse_octal(val);
                    }
                    Ok(())
                }

                _ => {
                    if let Some(ref val) = v {
                        self.options.insert(k.to_string(), val.to_string());
                    }
                    Ok(())
                }
            };

            if let Err(e) = result {
                return Err(e);
            }
        }
        Ok(())
    }

    fn parse_binding(&mut self, subtopic: &str, topic_override: bool) -> Result<(), ConfigError> {
        if self.broker.is_none() {
            return Err(ConfigError::Parse("broker needed before subtopic".to_string()));
        }

        let resolved_queue_name = self.resolve_queue_name();
        
        let mut full_topic = if topic_override {
            subtopic.to_string()
        } else {
            let mut parts = self.topic_prefix.clone();
            parts.push(subtopic.to_string());
            parts.join(".")
        };

        let scheme = self.broker.as_ref().unwrap().url.scheme().to_lowercase();
        if scheme == "mqtt" {
            full_topic = full_topic.replace('.', "/");
        }

        let exchange = self.resolve_exchange();
        
        let mut found = false;
        let broker_url = self.broker.as_ref().unwrap().url.to_string();
        for sub in &mut self.subscriptions {
            if let Some(cred) = &sub.broker {
                if cred.url.to_string() == broker_url && sub.queue.name == resolved_queue_name {
                    sub.bindings.push(Binding {
                        exchange: Some(exchange.clone()),
                        topic: full_topic.clone(),
                    });
                    found = true;
                    break;
                }
            }
        }

        if !found {
            let cred = Credential::new(self.broker.as_ref().unwrap().url.clone());
            self.subscriptions.push(Subscription::new(
                Some(cred),
                resolved_queue_name,
                self.queue_name.clone(),
                Some(exchange),
                full_topic,
            ));
        }

        Ok(())
    }

    fn parse_publisher(&mut self) {
        if let Some(broker) = &self.post_broker {
            let cred = Credential::new(broker.url.clone());
            let exchanges = self.resolve_post_exchanges();
            let topic_prefix = self.post_topic_prefix.clone()
                .or_else(|| Some(self.topic_prefix.clone()))
                .unwrap();
            let format = self.post_format.clone()
                .unwrap_or_else(|| topic_prefix.get(0).cloned().unwrap_or_else(|| "v03".to_string()));

            let mut publ = Publisher::new(Some(cred), exchanges, topic_prefix, format);
            publ.base_dir = self.post_base_dir.clone();
            publ.base_url = self.post_base_url.clone();

            if !self.publishers.contains(&publ) {
                self.publishers.push(publ);
            }
        }
    }

    fn resolve_post_exchanges(&self) -> Vec<String> {
        let mut exchange_root = self.post_exchange.clone()
            .unwrap_or_else(|| {
                if let Some(broker) = &self.post_broker {
                    if broker.url.scheme().starts_with("amqp") {
                        if let Some(user) = &broker.user {
                            if user != "anonymous" {
                                return format!("xs_{}", user);
                            }
                        }
                        return "xpublic".to_string();
                    }
                }
                "default".to_string()
            });

        if let Some(suffix) = &self.post_exchange_suffix {
            exchange_root = format!("{}_{}", exchange_root, suffix);
        }

        if self.post_exchange_split > 1 {
            let mut l = Vec::new();
            for i in 0..self.post_exchange_split {
                l.push(format!("{}{:02}", exchange_root, i));
            }
            l
        } else {
            vec![exchange_root]
        }
    }

    fn resolve_exchange(&self) -> String {
        if self.exchange != "xpublic" && self.exchange != "default" {
            return self.exchange.clone();
        }
        if let Some(broker) = &self.broker {
            if let Some(user) = &broker.user {
                if user != "anonymous" {
                    return format!("xs_{}", user);
                }
            }
        }
        "xpublic".to_string()
    }

    fn resolve_queue_name(&self) -> String {
        let mut vars = HashMap::new();
        if let Some(broker) = &self.broker {
            vars.insert("BROKER_USER".to_string(), broker.user.clone().unwrap_or_else(|| "anonymous".to_string()));
        } else {
            vars.insert("BROKER_USER".to_string(), "anonymous".to_string());
        }
        vars.insert("COMPONENT".to_string(), self.component.clone());
        vars.insert("CONFIG".to_string(), self.configname.clone().unwrap_or_else(|| "unknown".to_string()));
        
        let user = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
        let hostname = "localhost".to_string();
        let queue_share = format!("{}_{}_{}", user, hostname, self.rand8);
        
        vars.insert("QUEUESHARE".to_string(), queue_share);
        vars.insert("HOSTNAME".to_string(), hostname);
        vars.insert("USER".to_string(), user);
        vars.insert("RAND4".to_string(), self.rand4.clone());
        vars.insert("RAND8".to_string(), self.rand8.clone());

        variable_expansion::expand_variables(&self.queue_name, &vars)
    }

    fn include_file(&mut self, filename: &str) -> Result<(), ConfigError> {
        for base in &self.config_search_paths {
            let path = base.join(filename);
            if path.exists() {
                return self.read_file(path.to_str().unwrap());
            }
        }
        if Path::new(filename).exists() {
            return self.read_file(filename);
        }
        Err(ConfigError::FileNotFound(filename.to_string()))
    }

    pub fn finalize(&mut self) -> Result<(), ConfigError> {
        let mut vars = self.options.clone();
        vars.insert("APPNAME".to_string(), self.appname.clone());
        vars.insert("COMPONENT".to_string(), self.component.clone());
        if let Some(cn) = &self.configname {
            vars.insert("CONFIG".to_string(), cn.clone());
        }

        let cred_path = self.get_credential_path();
        let _ = self.credentials.load(&cred_path);

        if self.post_broker.is_some() {
            let broker_url = self.post_broker.as_ref().unwrap().url.to_string();
            if broker_url.contains('$') {
                let expanded = variable_expansion::expand_variables(&broker_url, &vars);
                self.post_broker = Some(Broker::parse(&expanded).map_err(|e| ConfigError::Parse(format!("post_broker finalize error: {}", e)))?);
            }
            self.parse_publisher();
        }

        if let Some(broker) = &mut self.broker {
            if broker.user.is_none() || broker.user.as_deref() == Some("anonymous") {
                if let Some(cred) = self.credentials.get(&broker.url.to_string()) {
                    let _ = broker.url.set_username(cred.url.username());
                    let _ = broker.url.set_password(cred.url.password());
                    broker.user = Some(cred.url.username().to_string());
                    broker.password = cred.url.password().map(String::from);
                } else if broker.user.is_none() {
                    let _ = broker.url.set_username("anonymous");
                    let _ = broker.url.set_password(Some("anonymous"));
                    broker.user = Some("anonymous".to_string());
                    broker.password = Some("anonymous".to_string());
                } else if broker.password.is_none() {
                    let _ = broker.url.set_password(Some("anonymous"));
                    broker.password = Some("anonymous".to_string());
                }
            }
        }

        if let Some(broker) = &mut self.post_broker {
            if broker.password.is_none() {
                if let Some(cred) = self.credentials.get(&broker.url.to_string()) {
                    broker.password = cred.url.password().map(String::from);
                }
            }
        }

        for sub in &mut self.subscriptions {
            if let Some(cred) = &mut sub.broker {
                if cred.url.password().is_none() {
                    if let Some(db_cred) = self.credentials.get(&cred.url.to_string()) {
                        let _ = cred.url.set_password(db_cred.url.password());
                    } else if cred.url.username() == "anonymous" || cred.url.username().is_empty() {
                        let _ = cred.url.set_username("anonymous");
                        let _ = cred.url.set_password(Some("anonymous"));
                    }
                }
            }
        }

        for publ in &mut self.publishers {
            if let Some(cred) = &mut publ.broker {
                if cred.url.password().is_none() {
                    if let Some(db_cred) = self.credentials.get(&cred.url.to_string()) {
                        let _ = cred.url.set_password(db_cred.url.password());
                    }
                }
            }
        }

        Ok(())
    }

    fn get_credential_path(&self) -> PathBuf {
        let path = paths::get_user_config_dir().join("credentials.conf");
        if path.exists() {
            return path;
        }
        PathBuf::from("credentials.conf")
    }
}

fn is_true(s: &str) -> bool {
    let s = s.to_lowercase();
    s == "true" || s == "yes" || s == "on" || s == "1"
}

fn parse_duration(s: &str) -> u32 {
    if s.is_empty() { return 0; }
    let last_char = s.chars().last().unwrap();
    if last_char.is_ascii_digit() {
        return s.parse().unwrap_or(0);
    }
    let val: u32 = s[..s.len()-1].parse().unwrap_or(0);
    match last_char {
        's' => val,
        'm' => val * 60,
        'h' => val * 3600,
        'd' => val * 86400,
        'w' => val * 604800,
        _ => val,
    }
}

fn parse_count(s: &str) -> u32 {
    if s.is_empty() { return 0; }
    let s = s.to_lowercase();
    let last_char = s.chars().last().unwrap();
    if last_char.is_ascii_digit() {
        return s.parse().unwrap_or(0);
    }
    
    if s.ends_with("kb") {
        return (s[..s.len()-2].parse::<f64>().unwrap_or(0.0) * 1024.0) as u32;
    }
    if s.ends_with("mb") {
        return (s[..s.len()-2].parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0) as u32;
    }
    if s.ends_with("gb") {
        return (s[..s.len()-2].parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0 * 1024.0) as u32;
    }

    let val = s[..s.len()-1].parse::<f64>().unwrap_or(0.0);
    match last_char {
        'k' => (val * 1000.0) as u32,
        'm' => (val * 1000000.0) as u32,
        'g' => (val * 1000000000.0) as u32,
        _ => s.parse().unwrap_or(0),
    }
}

fn parse_octal(s: &str) -> u32 {
    if s.starts_with('0') {
        u32::from_str_radix(&s[1..], 8).unwrap_or(0)
    } else {
        u32::from_str_radix(s, 8).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.appname, "sr3");
        assert_eq!(config.exchange, "xpublic");
    }

    #[test]
    fn test_parse_basic() {
        let mut config = Config::new();
        let content = "
            broker amqp://feeder@localhost/
            exchange xpublic
            subtopic v02.post.*.WXO-DD.model_gem_global.#
            accept .*model_gem_global.*
            directory /data/model_gem_global
            prefetch 25
        ";
        config.parse_string(content, "test.conf").unwrap();
        assert_eq!(config.exchange, "xpublic");
        assert_eq!(config.prefetch, 25);
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("300"), 300);
        assert_eq!(parse_duration("5m"), 300);
        assert_eq!(parse_duration("1h"), 3600);
        assert_eq!(parse_duration("1d"), 86400);
    }

    #[test]
    fn test_is_true() {
        assert!(is_true("true"));
        assert!(is_true("YES"));
        assert!(is_true("on"));
        assert!(is_true("1"));
        assert!(!is_true("false"));
    }

    #[test]
    fn test_include() {
        let dir = tempdir().unwrap();
        let main_path = dir.path().join("main.conf");
        let inc_path = dir.path().join("sub.inc");

        let mut main_file = File::create(&main_path).unwrap();
        writeln!(main_file, "exchange main_ex").unwrap();
        writeln!(main_file, "include sub.inc").unwrap();

        let mut inc_file = File::create(&inc_path).unwrap();
        writeln!(inc_file, "prefetch 42").unwrap();

        let mut config = Config::new();
        config.load(main_path.to_str().unwrap()).unwrap();

        assert_eq!(config.exchange, "main_ex");
        assert_eq!(config.prefetch, 42);
    }

    #[test]
    fn test_variable_expansion_in_config() {
        let mut config = Config::new();
        config.appname = "testapp".to_string();
        let content = "
            directory /data/${APPNAME}/${YYYYMMDD}
        ";
        config.parse_string(content, "test.conf").unwrap();
        let now = chrono::Utc::now();
        let expected = format!("/data/testapp/{}", now.format("%Y%m%d"));
        assert_eq!(config.directory, std::path::PathBuf::from(expected));
    }

    #[test]
    fn test_subscription_resolution() {
        let mut config = Config::new();
        config.configname = Some("test".to_string());
        config.parse_string("
            broker amqp://feeder@localhost/
            topicPrefix v02.post
            subtopic *.WXO-DD.#
        ", "test.conf").unwrap();
        
        assert_eq!(config.subscriptions.len(), 1);
        let sub = &config.subscriptions[0];
        assert_eq!(sub.bindings[0].topic, "v02.post.*.WXO-DD.#");
        assert_eq!(sub.bindings[0].exchange, Some("xs_feeder".to_string()));
        assert!(sub.queue.name.starts_with("q_feeder.flow.test."));
    }

    #[test]
    fn test_publisher_resolution() {
        let mut config = Config::new();
        config.parse_string("
            post_broker amqp://feeder@localhost/
            post_exchange xpublic
            post_topicPrefix v02.post
        ", "test.conf").unwrap();
        config.finalize().unwrap();
        
        assert_eq!(config.publishers.len(), 1);
        let publ = &config.publishers[0];
        assert_eq!(publ.exchange, vec!["xpublic".to_string()]);
        assert_eq!(publ.format, "v02");
    }

    #[test]
    fn test_parse_count() {
        assert_eq!(parse_count("100"), 100);
        assert_eq!(parse_count("1k"), 1000);
        assert_eq!(parse_count("1kb"), 1024);
        assert_eq!(parse_count("1.5mb"), 1572864);
    }

    #[test]
    fn test_parse_octal() {
        assert_eq!(parse_octal("0755"), 0o755);
        assert_eq!(parse_octal("644"), 0o644);
    }
}
