use rand::Rng;
use super::Identity;

pub struct Random;

impl Random {
    pub fn new() -> Self {
        Self
    }
}

impl Identity for Random {
    fn registered_as(&self) -> &'static str {
        "0"
    }

    fn get_method(&self) -> String {
        "random".to_string()
    }

    fn set_path(&mut self, _path: &str) {}

    fn update(&mut self, _chunk: &[u8]) {}

    fn value(&self) -> String {
        let mut rng = rand::thread_rng();
        format!("{:04}", rng.gen_range(0..10000))
    }
}
