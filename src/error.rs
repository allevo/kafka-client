use kafka_protocol::ResponseError;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Protocol(String),
    Config(String),
    NoBrokerAvailable(String),
    Authentication(String),
    Broker {
        error: ResponseError,
    },
    /// The per-request budget (`Config::request_timeout`) or the wrapping
    /// retry loop's `api_timeout` elapsed while the request was in flight.
    /// Classified as `RefreshMetadata`: a stalled node is treated like a
    /// disconnect, matching Java's `NetworkException` routing. We do not
    /// reuse `io::ErrorKind::TimedOut` here because that kind is `Retry`
    /// for generic I/O, while a request-budget expiry specifically implies
    /// "topology is suspect, re-resolve before retrying".
    RequestTimeout(String),
}

/// How a failed request should be handled by a retry loop.
///
/// Mirrors the Java client's exception hierarchy
/// (`RetriableException` / `InvalidMetadataException` /
/// `TransactionAbortableException` / fatal), which drives the
/// Sender/Fetcher retry state machines in
/// `clients/src/main/java/org/apache/kafka/clients/producer/internals/Sender.java`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAction {
    /// Retry the request as-is after backoff.
    Retry,
    /// Invalidate cached topology, refresh metadata, then retry.
    RefreshMetadata,
    /// Transactional producer must abort the current transaction; a new
    /// transaction may retry. No producer code inspects this yet; kept
    /// for forward-compat with `TRANSACTION_ABORTABLE` (Kafka 3.8+).
    Abortable,
    /// Propagate to the caller — do not retry.
    Fatal,
}

impl Error {
    /// Classify this error for retry decisions. See [`RetryAction`].
    pub fn classify(&self) -> RetryAction {
        match self {
            Error::Io(e) => classify_io_error(e.kind()),
            // Framing, correlation-id, or version-negotiation failures:
            // the wire is desynced or the broker is incompatible. Retry
            // cannot recover either.
            Error::Protocol(_) => RetryAction::Fatal,
            Error::Config(_) => RetryAction::Fatal,
            // No known broker address worked. A topology refresh is the
            // only thing that can unblock this.
            Error::NoBrokerAvailable(_) => RetryAction::RefreshMetadata,
            Error::Authentication(_) => RetryAction::Fatal,
            Error::Broker { error } => classify_response_error(*error),
            Error::RequestTimeout(_) => RetryAction::RefreshMetadata,
        }
    }

    pub fn is_retriable(&self) -> bool {
        matches!(
            self.classify(),
            RetryAction::Retry | RetryAction::RefreshMetadata
        )
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self.classify(), RetryAction::Fatal)
    }
}

/// Map `io::ErrorKind` to a retry action.
///
/// Disconnect-shaped kinds map to `RefreshMetadata` because in Kafka a
/// broker disconnect usually means the broker moved, was removed from
/// the cluster, or had its listener reconfigured — a metadata refresh
/// is the right recovery path (Java treats `NetworkException` as
/// `InvalidMetadataException`).
fn classify_io_error(kind: io::ErrorKind) -> RetryAction {
    use io::ErrorKind::*;
    match kind {
        ConnectionRefused | ConnectionReset | ConnectionAborted | BrokenPipe | NotConnected
        | UnexpectedEof | HostUnreachable | NetworkUnreachable | NetworkDown | AddrNotAvailable => {
            RetryAction::RefreshMetadata
        }

        TimedOut | WouldBlock | Interrupted => RetryAction::Retry,

        PermissionDenied | InvalidInput | InvalidData | AlreadyExists | NotFound | Unsupported => {
            RetryAction::Fatal
        }

        // Permissive default: unknown / `Other` kinds are treated as
        // transient, matching Java's fallthrough to `NetworkException`.
        _ => RetryAction::Retry,
    }
}

/// Map a broker-returned error code to a retry action.
///
/// Derived from Apache Kafka's exception hierarchy
/// (`clients/src/main/java/org/apache/kafka/common/errors/`). Do NOT
/// rebuild this on top of `ResponseError::is_retriable()` — that flag
/// only splits retriable from non-retriable and does not expose the
/// `InvalidMetadataException` subset or `TransactionAbortable`.
///
/// Unknown codes fall through to `Fatal` (fail-closed) rather than
/// `Retry`: if the client library does not recognise a code, it is not
/// safe to assume it is transient.
fn classify_response_error(e: ResponseError) -> RetryAction {
    use ResponseError::*;
    match e {
        // --- RefreshMetadata: Java `InvalidMetadataException` subclasses ---
        NotLeaderOrFollower
        | UnknownTopicOrPartition
        | UnknownTopicId
        | LeaderNotAvailable
        | ReplicaNotAvailable
        | PreferredLeaderNotAvailable
        | FencedLeaderEpoch
        | KafkaStorageError
        | ListenerNotFound
        | NetworkException
        | BrokerNotAvailable
        | RebootstrapRequired => RetryAction::RefreshMetadata,

        // --- Retry: Java `RetriableException`, non-metadata ---
        RequestTimedOut
        | CoordinatorLoadInProgress
        | CoordinatorNotAvailable
        | NotCoordinator
        | NotController
        | NotEnoughReplicas
        | NotEnoughReplicasAfterAppend
        | ConcurrentTransactions
        | ThrottlingQuotaExceeded
        | UnknownLeaderEpoch
        | FetchSessionIdNotFound
        | InvalidFetchSessionEpoch
        | InvalidShareSessionEpoch
        | ShareSessionNotFound
        | ShareSessionLimitReached
        | FetchSessionTopicIdError
        | CorruptMessage
        | OffsetNotAvailable
        | UnstableOffsetCommit
        | EligibleLeadersNotAvailable
        | ElectionNotNeeded
        | InconsistentTopicId => RetryAction::Retry,

        // --- Abortable: txn producer must abort and may retry in a new txn ---
        TransactionAbortable => RetryAction::Abortable,

        // Everything else — auth/authz, version/config, producer-idempotence,
        // group-membership, unknown codes — is fatal at the transport layer.
        // Higher-level producer/consumer logic may treat some of these
        // specially (e.g. REBALANCE_IN_PROGRESS triggers a rejoin, not a
        // wire-level retry); that routing happens above classify().
        _ => RetryAction::Fatal,
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {e}"),
            Error::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Error::Config(msg) => write!(f, "configuration error: {msg}"),
            Error::NoBrokerAvailable(msg) => write!(f, "no broker available: {msg}"),
            Error::Authentication(msg) => write!(f, "authentication error: {msg}"),
            Error::Broker { error } => write!(f, "broker error: {error}"),
            Error::RequestTimeout(msg) => write!(f, "request timeout: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Broker { error } => Some(error),
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
        Error::Protocol(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    fn io(kind: ErrorKind) -> Error {
        Error::Io(io::Error::from(kind))
    }

    #[test]
    fn io_disconnect_kinds_require_metadata_refresh() {
        for kind in [
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::BrokenPipe,
            ErrorKind::NotConnected,
            ErrorKind::UnexpectedEof,
        ] {
            assert_eq!(
                io(kind).classify(),
                RetryAction::RefreshMetadata,
                "{kind:?}"
            );
        }
    }

    #[test]
    fn io_timeout_is_plain_retry() {
        assert_eq!(io(ErrorKind::TimedOut).classify(), RetryAction::Retry);
    }

    #[test]
    fn io_permission_denied_is_fatal() {
        assert_eq!(
            io(ErrorKind::PermissionDenied).classify(),
            RetryAction::Fatal
        );
    }

    #[test]
    fn protocol_and_config_are_fatal() {
        assert!(Error::Protocol("desync".into()).is_fatal());
        assert!(Error::Config("bad".into()).is_fatal());
    }

    #[test]
    fn authentication_is_fatal() {
        assert!(Error::Authentication("nope".into()).is_fatal());
    }

    #[test]
    fn request_timeout_requests_metadata_refresh() {
        assert_eq!(
            Error::RequestTimeout("budget elapsed".into()).classify(),
            RetryAction::RefreshMetadata,
        );
    }

    #[test]
    fn no_broker_available_requests_metadata_refresh() {
        assert_eq!(
            Error::NoBrokerAvailable("none".into()).classify(),
            RetryAction::RefreshMetadata,
        );
    }

    fn broker(error: ResponseError) -> Error {
        Error::Broker { error }
    }

    #[test]
    fn broker_invalid_metadata_variants_require_refresh() {
        for e in [
            ResponseError::NotLeaderOrFollower,
            ResponseError::UnknownTopicOrPartition,
            ResponseError::UnknownTopicId,
            ResponseError::LeaderNotAvailable,
            ResponseError::FencedLeaderEpoch,
            ResponseError::KafkaStorageError,
            ResponseError::ListenerNotFound,
            ResponseError::NetworkException,
            ResponseError::BrokerNotAvailable,
        ] {
            assert_eq!(broker(e).classify(), RetryAction::RefreshMetadata, "{e:?}");
        }
    }

    #[test]
    fn broker_retriable_non_metadata_variants_retry() {
        for e in [
            ResponseError::RequestTimedOut,
            ResponseError::CoordinatorLoadInProgress,
            ResponseError::CoordinatorNotAvailable,
            ResponseError::NotCoordinator,
            ResponseError::NotController,
            ResponseError::ThrottlingQuotaExceeded,
            ResponseError::UnknownLeaderEpoch,
            ResponseError::CorruptMessage,
        ] {
            assert_eq!(broker(e).classify(), RetryAction::Retry, "{e:?}");
        }
    }

    #[test]
    fn broker_transaction_abortable_is_abortable() {
        assert_eq!(
            broker(ResponseError::TransactionAbortable).classify(),
            RetryAction::Abortable,
        );
    }

    #[test]
    fn broker_auth_and_version_are_fatal() {
        for e in [
            ResponseError::SaslAuthenticationFailed,
            ResponseError::ClusterAuthorizationFailed,
            ResponseError::TopicAuthorizationFailed,
            ResponseError::GroupAuthorizationFailed,
            ResponseError::TransactionalIdAuthorizationFailed,
            ResponseError::DelegationTokenAuthorizationFailed,
            ResponseError::DelegationTokenAuthDisabled,
            ResponseError::UnsupportedVersion,
            ResponseError::UnsupportedForMessageFormat,
            ResponseError::UnsupportedCompressionType,
            ResponseError::UnsupportedSaslMechanism,
            ResponseError::IllegalSaslState,
            ResponseError::InvalidConfig,
            ResponseError::InvalidRequest,
            ResponseError::InvalidRequiredAcks,
        ] {
            assert_eq!(broker(e).classify(), RetryAction::Fatal, "{e:?}");
        }
    }

    #[test]
    fn broker_unknown_code_is_fatal() {
        assert_eq!(
            broker(ResponseError::Unknown(-999)).classify(),
            RetryAction::Fatal
        );
        assert_eq!(
            broker(ResponseError::UnknownServerError).classify(),
            RetryAction::Fatal
        );
    }

    #[test]
    fn is_retriable_and_is_fatal_are_disjoint() {
        let cases = [
            io(ErrorKind::ConnectionReset),
            io(ErrorKind::TimedOut),
            io(ErrorKind::PermissionDenied),
            Error::Protocol("x".into()),
            Error::Config("x".into()),
            Error::NoBrokerAvailable("x".into()),
            Error::Authentication("x".into()),
            Error::RequestTimeout("x".into()),
            broker(ResponseError::NotLeaderOrFollower),
            broker(ResponseError::RequestTimedOut),
            broker(ResponseError::TransactionAbortable),
            broker(ResponseError::SaslAuthenticationFailed),
            broker(ResponseError::Unknown(-42)),
        ];
        for e in &cases {
            assert!(
                !(e.is_retriable() && e.is_fatal()),
                "classification overlaps: {e}",
            );
        }
    }
}
