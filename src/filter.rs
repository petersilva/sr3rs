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
    // Future: add strip, pstrip, flatten here
}

fn dummy_regex() -> Regex {
    Regex::new("").unwrap()
}

impl Filter {
    pub fn new(pattern: &str, accepting: bool, directory: PathBuf, mirror: bool) -> Result<Self, ConfigError> {
        let regex = Regex::new(pattern)?;
        Ok(Self {
            pattern: pattern.to_string(),
            regex,
            accepting,
            directory,
            mirror,
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
