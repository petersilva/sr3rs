//
// This file is part of sr3rs a rust implementation of Sarracenia. ( https://metpx.github.io/sarracenia )
// Copyright (C) Peter Silva, 2026
//

use regex::Regex;
use crate::config::ConfigError;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub pattern: String,
    #[serde(skip, default = "dummy_regex")]
    pub regex: Regex,
    pub accepting: bool,
    pub directory: PathBuf,
    pub mirror: bool,
    pub strip: Option<String>,
}

fn dummy_regex() -> Regex {
    Regex::new("").unwrap()
}

impl Filter {
    pub fn new(val: &str, accepting: bool, directory: PathBuf, mirror: bool) -> Result<Self, ConfigError> {
        let mut pattern = val.to_string();
        let mut strip = None;

        let parts: Vec<&str> = val.split_whitespace().collect();
        if !parts.is_empty() {
            pattern = parts[0].to_string();
            for part in &parts[1..] {
                if let Some((k, v)) = part.split_once('=') {
                    if k == "strip" {
                        strip = Some(v.to_string());
                    }
                }
            }
        }

        let regex = Regex::new(&pattern)?;
        Ok(Self {
            pattern,
            regex,
            accepting,
            directory,
            mirror,
            strip,
        })
    }

    pub fn matches(&self, text: &str) -> bool {
        self.regex.is_match(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_accept() {
        let filter = Filter::new(".*\\.txt", true, PathBuf::from("."), false).unwrap();
        assert!(filter.matches("test.txt"));
        assert!(!filter.matches("test.png"));
        assert!(filter.accepting);
    }
}
