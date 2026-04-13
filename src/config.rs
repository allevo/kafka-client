use std::sync::Arc;
use std::time::Duration;

/// Default maximum response size: 100 MiB.
pub const DEFAULT_MAX_RESPONSE_SIZE: usize = 100 * 1024 * 1024;

#[derive(Clone)]
pub enum Security {
    Plaintext,
    Ssl(Arc<rustls::ClientConfig>),
}

pub struct Config {
    pub host: String,
    pub port: u16,
    /// Maximum size (in bytes) of a single response frame from the broker. Larger frames
    /// cause the connection to be torn down. Defaults to [`DEFAULT_MAX_RESPONSE_SIZE`].
    pub max_response_size: usize,
    /// Maximum time `Connection::connect` may spend establishing the
    /// connection (TCP connect + TLS handshake). `None` disables the bound, in which
    /// case `connect` inherits OS defaults. A peer that accepts TCP but never sends
    /// TLS bytes can hang the caller indefinitely.
    pub connection_setup_timeout: Option<Duration>,
}

impl Config {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Config {
            host: host.into(),
            port,
            max_response_size: DEFAULT_MAX_RESPONSE_SIZE,
            connection_setup_timeout: None,
        }
    }

    pub fn with_max_response_size(mut self, size: usize) -> Self {
        self.max_response_size = size;
        self
    }

    pub fn with_connection_setup_timeout(mut self, timeout: Duration) -> Self {
        self.connection_setup_timeout = Some(timeout);
        self
    }
}
