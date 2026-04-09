/// Default maximum response size: 100 MiB.
pub const DEFAULT_MAX_RESPONSE_SIZE: usize = 100 * 1024 * 1024;

pub struct Config {
    pub host: String,
    pub port: u16,
    /// Maximum size (in bytes) of a single response frame from the broker. Larger frames
    /// cause the connection to be torn down. Defaults to [`DEFAULT_MAX_RESPONSE_SIZE`].
    pub max_response_size: usize,
}

impl Config {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Config {
            host: host.into(),
            port,
            max_response_size: DEFAULT_MAX_RESPONSE_SIZE,
        }
    }

    pub fn with_max_response_size(mut self, size: usize) -> Self {
        self.max_response_size = size;
        self
    }
}
