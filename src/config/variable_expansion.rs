use regex::{Regex, Captures};
use chrono::Utc;
use std::collections::HashMap;
use lazy_static::lazy_static;

lazy_static! {
    static ref VAR_RE: Regex = Regex::new(r"\$\{(?P<var>[^}]+)\}").unwrap();
    static ref STRFTIME_RE: Regex = Regex::new(r"%\w").unwrap();
}

pub fn expand_variables(text: &str, vars: &HashMap<String, String>) -> String {
    let now = Utc::now();
    
    let result = VAR_RE.replace_all(text, |caps: &Captures| {
        let var = &caps["var"];
        
        // Handle strftime patterns directly if they start with %
        if var.starts_with('%') {
            return now.format(var).to_string();
        }

        // Handle predefined variables
        match var {
            "YYYYMMDD" => now.format("%Y%m%d").to_string(),
            "YYYY" => now.format("%Y").to_string(),
            "MM" => now.format("%m").to_string(),
            "DD" => now.format("%d").to_string(),
            "HH" => now.format("%H").to_string(),
            "JJJ" => now.format("%j").to_string(),
            _ => {
                // Look up in provided variables
                vars.get(var).cloned().unwrap_or_else(|| caps[0].to_string())
            }
        }
    });

    result.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_expansion() {
        let mut vars = HashMap::new();
        vars.insert("SOURCE".to_string(), "mysource".to_string());
        
        let input = "/data/${SOURCE}/${YYYYMMDD}/file.txt";
        let expanded = expand_variables(input, &vars);
        
        let now = Utc::now();
        let expected = format!("/data/mysource/{}/file.txt", now.format("%Y%m%d"));
        assert_eq!(expanded, expected);
    }

    #[test]
    fn test_strftime_expansion() {
        let vars = HashMap::new();
        let input2 = "path/${%Y-%m}";
        let expanded = expand_variables(input2, &vars);
        let now = Utc::now();
        let expected = format!("path/{}", now.format("%Y-%m"));
        assert_eq!(expanded, expected);
    }
}
