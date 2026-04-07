use super::Identity;
use std::sync::RwLock;
use lazy_static::lazy_static;

lazy_static! {
    static ref DEFAULT_VALUE: RwLock<String> = RwLock::new("None".to_string());
}

pub fn set_default_value(value: String) {
    if let Ok(mut default) = DEFAULT_VALUE.write() {
        *default = value;
    }
}

pub struct Arbitrary {
    value: String,
}

impl Arbitrary {
    pub fn new() -> Self {
        let value = DEFAULT_VALUE.read().unwrap().clone();
        Self { value }
    }

    pub fn set_value(&mut self, value: String) {
        self.value = value;
    }
}

impl Identity for Arbitrary {
    fn registered_as(&self) -> &'static str {
        "a"
    }

    fn get_method(&self) -> String {
        "arbitrary".to_string()
    }

    fn set_path(&mut self, _path: &str) {}

    fn update(&mut self, _chunk: &[u8]) {}

    fn value(&self) -> String {
        self.value.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init() {
        // Reset to "None" in case other tests modified it due to parallel execution
        set_default_value("None".to_string());
        let hash = Arbitrary::new();
        assert_eq!(hash.value(), "None");
    }

    #[test]
    fn test_registered_as() {
        let hash = Arbitrary::new();
        assert_eq!(hash.registered_as(), "a");
    }

    #[test]
    fn test_set_path() {
        let mut hash = Arbitrary::new();
        hash.set_path("dummy_path.txt");
        // Passed if no panic
    }

    #[test]
    fn test_update() {
        let mut hash = Arbitrary::new();
        hash.update(b"randomstring");
        // Passed if no panic
    }

    #[test]
    fn test_property_value() {
        set_default_value("default".to_string());
        
        let mut hash = Arbitrary::new();
        assert_eq!(hash.value(), "default");

        hash.set_value("new".to_string());
        assert_eq!(hash.value(), "new");
        
        // Reset state
        set_default_value("None".to_string());
    }
}
