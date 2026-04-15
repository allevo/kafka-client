use std::sync::Arc;
use std::time::Duration;

/// Default maximum response size: 100 MiB.
pub const DEFAULT_MAX_RESPONSE_SIZE: usize = 100 * 1024 * 1024;

/// Base reconnect backoff (librdkafka default, friendlier than Java's 50 ms
/// to a recovering broker). See `Config::reconnect_backoff`.
pub const DEFAULT_RECONNECT_BACKOFF: Duration = Duration::from_millis(100);

/// Cap on reconnect backoff (librdkafka default). See
/// `Config::reconnect_backoff_max`.
pub const DEFAULT_RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(10);

/// Default per-request budget, covering broker acquisition + wire exchange.
/// Matches Java's `request.timeout.ms` default (30 s).
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Default wall-clock cap on `Client::send`'s retry loop. Matches Java's
/// `default.api.timeout.ms` (60 s).
pub const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(60);

/// Default maximum retry count for `Client::send`. Java effectively defaults
/// to `Integer.MAX_VALUE` gated on `delivery.timeout.ms`; without a
/// delivery-timeout concept yet, a small explicit cap is less surprising.
pub const DEFAULT_RETRIES: u32 = 3;

/// Default base backoff between `Client::send` retry attempts
/// (Java `retry.backoff.ms`, 100 ms).
pub const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Default cap on the retry backoff (Java `retry.backoff.max.ms`, 1 s).
pub const DEFAULT_RETRY_BACKOFF_MAX: Duration = Duration::from_secs(1);

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
    /// Idle connection time. `None` disables the check.
    /// Recommended: slightly below the broker's own idle close (default 10 min).
    /// Re-auth flow reset the timer.
    pub connections_max_idle: Option<Duration>,
    /// Base reconnect backoff applied after a failed dial to a broker. The
    /// next dial to the same broker-id is gated by at least this duration
    /// (plus jitter) after the previous failure.
    pub reconnect_backoff: Duration,
    /// Upper bound on the exponential reconnect backoff.
    pub reconnect_backoff_max: Duration,
    /// Per-request budget covering both broker acquisition (dialer park or
    /// fast-path clone) and the terminal wire exchange. On elapse, the
    /// target broker connection is torn down and `Error::RequestTimeout`
    /// is returned. Mirrors Java's `request.timeout.ms`.
    pub request_timeout: Duration,
    /// Wall-clock cap on `Client::send`'s retry loop. The loop gives up
    /// with `Error::RequestTimeout` once this elapses, regardless of how
    /// many attempts remain. Mirrors Java's `default.api.timeout.ms`.
    pub api_timeout: Duration,
    /// Maximum retry count for `Client::send` after the first attempt.
    /// `0` disables the retry loop.
    pub retries: u32,
    /// Base backoff between `Client::send` retry attempts. Distinct from
    /// `reconnect_backoff*` (which governs dialer retries on a failed
    /// TCP/TLS handshake). Mirrors Java's `retry.backoff.ms`.
    pub retry_backoff: Duration,
    /// Cap on the exponential retry backoff. Mirrors Java's
    /// `retry.backoff.max.ms`.
    pub retry_backoff_max: Duration,
}

impl Config {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Config {
            host: host.into(),
            port,
            max_response_size: DEFAULT_MAX_RESPONSE_SIZE,
            connection_setup_timeout: None,
            connections_max_idle: None,
            reconnect_backoff: DEFAULT_RECONNECT_BACKOFF,
            reconnect_backoff_max: DEFAULT_RECONNECT_BACKOFF_MAX,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            api_timeout: DEFAULT_API_TIMEOUT,
            retries: DEFAULT_RETRIES,
            retry_backoff: DEFAULT_RETRY_BACKOFF,
            retry_backoff_max: DEFAULT_RETRY_BACKOFF_MAX,
        }
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn with_api_timeout(mut self, timeout: Duration) -> Self {
        self.api_timeout = timeout;
        self
    }

    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    pub fn with_retry_backoff(mut self, base: Duration) -> Self {
        self.retry_backoff = base;
        self
    }

    pub fn with_retry_backoff_max(mut self, max: Duration) -> Self {
        self.retry_backoff_max = max;
        self
    }

    pub fn with_reconnect_backoff(mut self, base: Duration) -> Self {
        self.reconnect_backoff = base;
        self
    }

    pub fn with_reconnect_backoff_max(mut self, max: Duration) -> Self {
        self.reconnect_backoff_max = max;
        self
    }

    pub fn with_max_response_size(mut self, size: usize) -> Self {
        self.max_response_size = size;
        self
    }

    pub fn with_connection_setup_timeout(mut self, timeout: Duration) -> Self {
        self.connection_setup_timeout = Some(timeout);
        self
    }

    pub fn with_connections_max_idle(mut self, d: Duration) -> Self {
        self.connections_max_idle = Some(d);
        self
    }
}
