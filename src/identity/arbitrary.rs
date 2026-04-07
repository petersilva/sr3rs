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
