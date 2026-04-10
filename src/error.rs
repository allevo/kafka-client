use std::fmt;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    ProtocolError(String),
    AuthenticationError(String),
    ApiError { error_code: i16 },
    ReauthenticationFailed(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::ProtocolError(msg) => write!(f, "protocol error: {msg}"),
            Error::AuthenticationError(msg) => write!(f, "authentication error: {msg}"),
            Error::ApiError { error_code } => write!(f, "broker error code: {error_code}"),
            Error::ReauthenticationFailed(msg) => write!(f, "re-authentication failed: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::ProtocolError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
