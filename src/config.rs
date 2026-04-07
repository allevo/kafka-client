pub struct Config {
    pub host: String,
    pub port: u16,
}

impl Config {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Config {
            host: host.into(),
            port,
        }
    }
}
