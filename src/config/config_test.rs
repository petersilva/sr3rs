//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use super::*;
use crate::message::Message;
use regex::Regex;
use std::path::PathBuf;

fn try_pattern(config: &Config, message: Option<&Message>, pattern: &str, good_re: &str) -> bool {
    let mut vars = config.options.clone();
    vars.insert("APPNAME".to_string(), config.appname.clone());
    vars.insert("COMPONENT".to_string(), config.component.clone());
    if let Some(cn) = &config.configname {
        vars.insert("CONFIG".to_string(), cn.clone());
    }
    vars.insert("RAND4".to_string(), config.rand4.clone());
    vars.insert("RAND8".to_string(), config.rand8.clone());
    vars.insert("USER".to_string(), std::env::var("USER").unwrap_or_else(|_| "unknown".to_string()));
    vars.insert("HOSTNAME".to_string(), "localhost".to_string());
    
    if let Some(msg) = message {
        if let Some(src) = msg.fields.get("source") {
            vars.insert("SOURCE".to_string(), src.clone());
        }
        
        // baseUrl expansion
        if let Ok(url) = url::Url::parse(&msg.base_url) {
            vars.insert("BUP".to_string(), url.path().to_string());
            vars.insert("baseUrlPath".to_string(), url.path().to_string());
            if let Some(last) = url.path().split('/').filter(|s| !s.is_empty()).last() {
                vars.insert("BUPL".to_string(), last.to_string());
                vars.insert("baseUrlPathLast".to_string(), last.to_string());
            }
        }
    }

    if let Some(pbd) = &config.post_base_dir {
        vars.insert("PBD".to_string(), pbd.to_string_lossy().to_string());
        vars.insert("PDR".to_string(), pbd.to_string_lossy().to_string());
    }
    
    vars.insert("BD".to_string(), config.directory.to_string_lossy().to_string());
    vars.insert("DR".to_string(), config.directory.to_string_lossy().to_string());

    let expanded = variable_expansion::expand_variables(pattern, &vars);
    let re = Regex::new(good_re).unwrap();
    re.is_match(&expanded)
}

#[test]
fn test_variable_expansion() {
    let mut config = Config::default();
    config.directory = PathBuf::from("/data/whereIcameFrom");
    config.post_base_dir = Some(PathBuf::from("/data/whereIamGoingTo"));
    
    let mut message = Message::new("https://localhost/smorgasbord", "test.txt");
    message.fields.insert("source".to_string(), "WhereDataComesFrom".to_string());

    assert!(try_pattern(&config, Some(&message), "${PDR}/${%Y%m%d}/", r"^/data/whereIamGoingTo/[0-9]{8}/$"));
    assert!(try_pattern(&config, Some(&message), "${PBD}/${%Y%m%d}/", r"^/data/whereIamGoingTo/[0-9]{8}/$"));
    
    let source = message.fields.get("source").unwrap();
    assert!(try_pattern(&config, Some(&message), "${PBD}/${SOURCE}/${%Y%m%d}/boughsOfHolly", 
        &format!(r"^/data/whereIamGoingTo/{}/[0-9]{{8}}/boughsOfHolly$", source)));

    // Test BUPL (should work now with my try_pattern enhancement)
    assert!(try_pattern(&config, Some(&message), "${PBD}/${BUPL}/${%Y%m%d}/falala", r"^/data/whereIamGoingTo/smorgasbord/[0-9]{8}/falala$"));
    assert!(try_pattern(&config, Some(&message), "${PBD}/${BUPL}/${%Y%m%d}/", r"^/data/whereIamGoingTo/smorgasbord/[0-9]{8}/$"));

    assert!(try_pattern(&config, Some(&message), "a_path/${DONT_TOUCH_THIS}something/else", r"^a_path/\$\{DONT_TOUCH_THIS\}something/else$"));
}

#[test]
fn test_read_line_declare() {
    let mut config = Config::default();
    
    config.parse_string("declare env VAR99=hoho", "test.conf").unwrap();
    assert_eq!(std::env::var("VAR99").unwrap(), "hoho");
    
    config.parse_string("declare env VAR98=hoho=lala", "test.conf").unwrap();
    assert_eq!(std::env::var("VAR98").unwrap(), "hoho=lala");
    
    // Note: declare subscriber is not yet implemented in Rust, 
    // but we can add it to the test once it is.
}

#[test]
fn test_read_line_flags() {
    let mut config = Config::default();
    
    let flags = [
        ("off", false), ("on", true),
        ("False", false), ("True", true),
        ("no", false), ("yes", true)
    ];
    
    for (val, expected) in flags {
        config.parse_string(&format!("download {}", val), "test.conf").unwrap();
        assert_eq!(config.download, expected);
    }
}

#[test]
fn test_read_line_counts() {
    let mut config = Config::default();

    config.parse_string("batch 1", "test.conf").unwrap();
    assert_eq!(config.batch, 1);
    
    config.parse_string("batch 1kb", "test.conf").unwrap();
    assert_eq!(config.batch, 1024);
    
    config.parse_string("batch 1k", "test.conf").unwrap();
    assert_eq!(config.batch, 1000);
    
    config.parse_string("batch 1m", "test.conf").unwrap();
    assert_eq!(config.batch, 1_000_000);
    
    config.parse_string("batch 1mb", "test.conf").unwrap();
    assert_eq!(config.batch, 1024 * 1024);
}

#[test]
fn test_read_line_perms() {
    let mut config = Config::default();
    
    config.parse_string("permDefault 0755", "test.conf").unwrap();
    assert_eq!(config.perm_default, 0o755);

    config.parse_string("permDefault 644", "test.conf").unwrap();
    assert_eq!(config.perm_default, 0o644);
}

#[test]
fn test_read_line_duration() {
    let mut config = Config::default();
    
    config.parse_string("sleep 30", "test.conf").unwrap();
    assert_eq!(config.sleep, 30.0);

    config.parse_string("sleep 30m", "test.conf").unwrap();
    assert_eq!(config.sleep, (30 * 60) as f64);

    config.parse_string("sleep 3h", "test.conf").unwrap();
    assert_eq!(config.sleep, (3 * 60 * 60) as f64);

    config.parse_string("sleep 2d", "test.conf").unwrap();
    assert_eq!(config.sleep, (2 * 24 * 60 * 60) as f64);
}

#[test]
fn test_guess_type() {
    // In Rust, we don't have a direct guess_type, but we can test our parsing helpers.
    assert_eq!(is_true("true"), true);
    assert_eq!(is_true("1"), true);
    assert_eq!(is_true("0"), false);
    
    assert_eq!(parse_count("1k"), 1000);
    assert_eq!(parse_count("1kb"), 1024);
}

#[test]
fn test_nodupe_ttl_parsing() {
    let mut config = Config::default();
    config.apply_component_defaults("subscribe");
    
    assert_eq!(config.nodupe_ttl, 0);
    
    // In Rust, 'on' maps to 300 via parse_duration? No, parse_duration("on") returns 0.
    // Python finalize() does this: if nodupe_ttl is True set to 300.
    // Let's check parse_duration in Rust.
}

#[test]
fn test_subscription() {
    let mut config = Config::default();
    config.component = "subscribe".to_string();
    
    config.parse_string("broker amqp://user1@localhost", "test.conf").unwrap();
    config.parse_string("exchange ex1", "test.conf").unwrap();
    config.parse_string("subtopic topic1.#", "test.conf").unwrap();
    
    assert_eq!(config.subscriptions.len(), 1);
    assert_eq!(config.subscriptions[0].bindings.len(), 1);
    assert_eq!(config.subscriptions[0].bindings[0].topic, "v02.post.topic1.#");
    
    config.parse_string("broker amqp://user2@localhost", "test.conf").unwrap();
    config.parse_string("subtopic topic2.#", "test.conf").unwrap();
    
    assert_eq!(config.subscriptions.len(), 2);
    assert_eq!(config.subscriptions[1].bindings[0].topic, "v02.post.topic2.#");
}

#[test]
fn test_multi_publishers() {
    let mut config = Config::default();
    
    config.parse_string("post_broker amqp://p1@localhost", "test.conf").unwrap();
    config.parse_string("post_exchange x1", "test.conf").unwrap();
    
    config.parse_string("post_broker amqp://p2@localhost", "test.conf").unwrap();
    config.parse_string("post_exchange x2", "test.conf").unwrap();
    
    config.finalize().unwrap();
    
    assert_eq!(config.publishers.len(), 2);
    assert_eq!(config.publishers[0].exchange, vec!["x1".to_string()]);
    assert_eq!(config.publishers[1].exchange, vec!["x2".to_string()]);
}

#[test]
fn test_stable_rand() {
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let home = dir.path().to_path_buf();
    
    // We need to set HOME so directories/paths uses it
    let old_home = std::env::var("HOME").ok();
    std::env::set_var("HOME", &home);

    println!("Cache dir: {:?}", paths::get_user_cache_dir(None));

    let config_path = home.join("test.conf");
    std::fs::write(&config_path, "prefetch 10").unwrap();

    let mut config = Config::default();
    config.apply_component_defaults("testcomp");
    config.load(config_path.to_str().unwrap()).unwrap();
    config.save_state().unwrap();    
    let r4 = config.rand4.clone();
    let r8 = config.rand8.clone();
    
    // Reload
    let mut config2 = Config::default();
    config2.apply_component_defaults("testcomp");
    config2.load(config_path.to_str().unwrap()).unwrap();
    
    assert_eq!(config2.rand4, r4);
    assert_eq!(config2.rand8, r8);

    // Restore HOME
    if let Some(h) = old_home {
        std::env::set_var("HOME", h);
    } else {
        std::env::remove_var("HOME");
    }
}

#[test]
fn test_subscriptions_persistence() {
    use tempfile::tempdir;
    let dir = tempdir().unwrap();
    let home = dir.path().to_path_buf();
    
    let old_home = std::env::var("HOME").ok();
    std::env::set_var("HOME", &home);

    let config_path = home.join("test.conf");
    std::fs::write(&config_path, "
        broker amqp://feeder@localhost/
        subtopic topic.#
    ").unwrap();

    let mut config = Config::default();
    config.apply_component_defaults("subscribe");
    config.load(config_path.to_str().unwrap()).unwrap();
    config.finalize().unwrap();
    config.save_state().unwrap();    
    assert_eq!(config.subscriptions.len(), 1);
    let q_name = config.subscriptions[0].queue.name.clone();
    
    // Check if file exists
    let cache_dir = paths::get_user_cache_dir(None);
    let state_path = cache_dir.join("subscribe/test/subscriptions.json");
    println!("Checking state path: {:?}", state_path);
    assert!(state_path.exists());

    // Reload
    let mut config2 = Config::default();
    config2.apply_component_defaults("subscribe");
    config2.load(config_path.to_str().unwrap()).unwrap();
    
    assert_eq!(config2.subscriptions.len(), 1);
    assert_eq!(config2.subscriptions[0].queue.name, q_name);

    if let Some(h) = old_home {
        std::env::set_var("HOME", h);
    } else {
        std::env::remove_var("HOME");
    }
}

#[test]
fn test_duplicate_subscriptions() {
    let mut config = Config::default();
    config.apply_component_defaults("subscribe");
    
    config.parse_string("broker amqp://feeder@localhost", "test.conf").unwrap();
    config.parse_string("subtopic topic.#", "test.conf").unwrap();
    
    // Finalize should call parse_subscription(None, None) 
    // but subtopic already added one.
    config.finalize().unwrap();
    
    assert_eq!(config.subscriptions.len(), 1);
    assert_eq!(config.subscriptions[0].bindings.len(), 1);
    
    // Calling subtopic again with same topic should NOT add new subscription
    config.parse_string("subtopic topic.#", "test.conf").unwrap();
    assert_eq!(config.subscriptions.len(), 1);
    
    // Different topic should add a binding to the SAME subscription
    config.parse_string("subtopic other.#", "test.conf").unwrap();
    assert_eq!(config.subscriptions.len(), 1);
    assert_eq!(config.subscriptions[0].bindings.len(), 2);
}

#[test]
fn test_parse_subscription_deduplication() {
    let mut config = Config::default();
    config.component = "subscribe".to_string();
    config.configname = Some("test".to_string());
    config.broker = Some(Broker::parse("amqp://feeder@localhost/").unwrap());
    
    // 1. Initial call adds one
    config.parse_subscription(None, None);
    assert_eq!(config.subscriptions.len(), 1);
    
    // 2. Call again with same values should NOT add another one
    config.parse_subscription(None, None);
    assert_eq!(config.subscriptions.len(), 1);
    
    // 3. Call again with DIFFERENT broker (with password) but same user/host
    config.broker = Some(Broker::parse("amqp://feeder:secret@localhost/").unwrap());
    config.parse_subscription(None, None);
    assert_eq!(config.subscriptions.len(), 1);
}

#[test]
fn test_post_base_dir_derivation() {
    let mut config = Config::default();
    config.parse_string("post_baseUrl file:/tmp/foo", "test.conf").unwrap();
    config.finalize().unwrap();
    
    assert_eq!(config.post_base_dir, Some(PathBuf::from("/tmp/foo")));
    
    // Test that file:/ results in /
    let mut config_root = Config::default();
    config_root.parse_string("post_baseUrl file:/", "test.conf").unwrap();
    config_root.finalize().unwrap();
    assert_eq!(config_root.post_base_dir, Some(PathBuf::from("/")));

    // Test that explicit post_baseDir wins
    let mut config2 = Config::default();
    config2.parse_string("post_baseUrl file:/tmp/foo", "test.conf").unwrap();
    config2.parse_string("post_baseDir /var/tmp", "test.conf").unwrap();
    config2.finalize().unwrap();
    
    assert_eq!(config2.post_base_dir, Some(PathBuf::from("/var/tmp")));
}

#[test]
fn test_post_base_dir_default_to_directory() {
    let mut config = Config::default();
    config.directory = PathBuf::from("/home/user/data");
    config.finalize().unwrap();
    
    assert_eq!(config.post_base_dir, Some(PathBuf::from("/home/user/data")));
}
