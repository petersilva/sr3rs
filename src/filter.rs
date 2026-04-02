use regex::Regex;
use crate::config::ConfigError;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Filter {
    pub pattern: String,
    #[serde(skip, default = "dummy_regex")]
    pub regex: Regex,
    pub accepting: bool,
}

fn dummy_regex() -> Regex {
    Regex::new("").unwrap()
}

impl Filter {
    pub fn new(pattern: &str, accepting: bool) -> Result<Self, ConfigError> {
        let regex = Regex::new(pattern)?;
        Ok(Self {
            pattern: pattern.to_string(),
            regex,
            accepting,
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
        let filter = Filter::new(".*\\.txt", true).unwrap();
        assert!(filter.matches("test.txt"));
        assert!(!filter.matches("test.png"));
        assert!(filter.accepting);
    }

    #[test]
    fn test_filter_reject() {
        let filter = Filter::new(".*\\.tmp", false).unwrap();
        assert!(filter.matches("data.tmp"));
        assert!(!filter.matches("data.txt"));
        assert!(!filter.accepting);
    }

    #[test]
    fn test_invalid_regex() {
        let result = Filter::new("[unclosed bracket", true);
        assert!(result.is_err());
    }
}
