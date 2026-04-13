use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, LockResult, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{
    ApiKey, MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

use zeropool::BufferPool;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::secret::SecretString;

#[derive(Clone)]
pub enum Auth {
    None,
    Plain {
        username: String,
        password: SecretString,
    },
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct CorrelationId(i32);
impl Deref for CorrelationId {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl TryFrom<&[u8]> for CorrelationId {
    type Error = &'static str;

    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        if value.len() < 4 {
            return Err("Buffer not enough length");
        }
        let buff: [u8; 4] = [value[0], value[1], value[2], value[3]];
        Ok(Self(i32::from_be_bytes(buff)))
    }
}

struct RequestMsg {
    correlation_id: CorrelationId,
    data: zeropool::PooledBuffer,
    response_tx: oneshot::Sender<Result<zeropool::PooledBuffer>>,
}

type InFlight =
    Arc<Mutex<VecDeque<(CorrelationId, oneshot::Sender<Result<zeropool::PooledBuffer>>)>>>;

#[derive(Clone)]
pub struct BrokerClient {
    // Stable per-instance identity assigned at construction.
    id: u64,
    request_tx: mpsc::Sender<RequestMsg>,
    next_correlation_id: Arc<AtomicI32>,
    pool: BufferPool,
    api_versions: Arc<[ApiVersion]>,
    // Flipped by `read_task` the moment it decides to exit, so `send_raw` can
    // reject new requests immediately instead of letting them sit in the mpsc
    // channel until `write_task` notices the shutdown signal and drops `request_rx`.
    shutdown: Arc<AtomicBool>,
    // Holds the reauth-shutdown sender alive when no reauth task is running
    _reauth_shutdown_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

const CLIENT_ID: &str = "kafka-client";

impl BrokerClient {
    /// Build a `BrokerClient` from an established `Connection`, performing initial SASL auth
    /// (if requested) and spawning a background re-auth task when the broker reports a
    /// non-zero `session_lifetime_ms`.
    pub async fn new(mut connection: Connection, auth: Auth) -> Result<Self> {
        let mut correlation_id: i32 = 0;

        let api_versions = connection.fetch_api_versions(&mut correlation_id).await?;

        let pool = BufferPool::new();
        let max_response_size = connection.max_response_size;

        // Split the stream and build the shared in-flight map up front, so the read and
        // write tasks are siblings spawned.
        let (reader, writer) = tokio::io::split(connection.stream);
        let (request_tx, request_rx) = mpsc::channel::<RequestMsg>(32);

        // Invariant: kafka guarantees in-order responses on a single connection.
        // Therefore:
        // - `write_task` push back requests
        // - `read_task` pop front
        // It is easier and faster than HashMap
        // See: https://kafka.apache.org/42/design/protocol/#network
        let in_flight: InFlight = Arc::new(Mutex::new(VecDeque::new()));

        // Connect `write_task` to `read_task` to communicate when the shutdown happens
        let (read_shutdown_tx, read_shutdown_rx) = oneshot::channel::<()>();
        // Lets `reauth_task` signal `read_task` to tear down the connection on failure.
        let (reauth_shutdown_tx, reauth_shutdown_rx) = oneshot::channel::<()>();
        let shutdown = Arc::new(AtomicBool::new(false));

        tracing::info!("spawning write/read tasks");
        tokio::spawn(write_task(
            writer,
            request_rx,
            in_flight.clone(),
            read_shutdown_rx,
        ));
        tokio::spawn(read_task(
            reader,
            in_flight,
            max_response_size,
            read_shutdown_tx,
            shutdown.clone(),
            reauth_shutdown_rx,
            pool.clone(),
        ));

        let client = BrokerClient {
            id: fastrand::u64(..),
            request_tx,
            pool,
            next_correlation_id: Arc::new(AtomicI32::new(correlation_id + 1)),
            api_versions: api_versions.into(),
            shutdown,
            // Keep the sender alive so the oneshot in read_task stays pending.
            // Moved into reauth_task below if reauth is needed.
            _reauth_shutdown_tx: Arc::new(Mutex::new(None)),
        };

        let session_lifetime = client.authenticate(&auth).await?;

        // KIP-368: brokers with connections.max.reauth.ms > 0 will kill connections that
        // don't re-authenticate before the session expires. Spawn a background task that
        // sleeps and re-authenticates periodically.
        if let Some(lifetime) = session_lifetime {
            tracing::info!(?lifetime, "spawning re-auth task");
            tokio::spawn(reauth_task(
                client.clone(),
                auth,
                lifetime,
                reauth_shutdown_tx,
            ));
        } else {
            tracing::info!("Broker doesn't require re-auth task");
            // Store reauth_shutdown_tx, so the `read_task` can poll it even if it will never be resolved
            // This make the code easier at a little cost of a `tokio::sync::oneshot::Receiver::poll`
            let mut guard = client._reauth_shutdown_tx.lock().unwrap();
            *guard = Some(reauth_shutdown_tx);
        }

        Ok(client)
    }

    /// Send a typed Kafka request and decode the response.
    pub async fn send<Req, Resp>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request: Req,
    ) -> Result<Resp>
    where
        Req: Encodable + HeaderVersion + Send + 'static,
        Resp: Decodable + HeaderVersion,
    {
        // Fast-path rejection once `read_task` has decided to exit.
        // NB: a small unavoidable TOCTOU window is still possibile.
        //     But it is temporary.
        // So this flag is a best-effort fast path; In the TOCTOU window
        // there's a safety net.
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "broker connection closed",
            )));
        }

        // correlation_id is a client side parameter. There is not guarantees of the values
        // No lock is needed.
        let correlation_id =
            CorrelationId(self.next_correlation_id.fetch_add(1, Ordering::Relaxed));

        // Below:
        // - Serialize the request
        //   It is made here to not pay the serialization cost "globally"
        // - Send the request to the background task `write_task`
        // - Wait for the response
        // - Deserialize the respone
        //   It is made here to not pay the cost "globally"

        let header = RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(*correlation_id)
            .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));

        let header_version = Req::header_version(api_version);
        let size = header.compute_size(header_version)? + request.compute_size(api_version)?;
        let mut buf = self.pool.get(4 + size);
        debug_assert_eq!(buf.len(), 4 + size);
        // We want to start from position 0, discarding dirty (and old) values
        buf.clear();
        buf.put_i32(size as i32);
        header.encode(&mut *buf, header_version)?;
        request.encode(&mut *buf, api_version)?;
        let data = buf;

        tracing::debug!(?correlation_id, bytes = data.len(), "sending request");

        let (response_tx, response_rx) = oneshot::channel();

        let request_msg = RequestMsg {
            correlation_id,
            data,
            response_tx,
        };

        self.request_tx.send(request_msg).await.map_err(|_| {
            tracing::error!("connection task has shut down");
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "connection task has shut down",
            ))
        })?;

        let result = response_rx.await.map_err(|_| {
            tracing::error!(
                ?correlation_id,
                "connection task dropped without responding"
            );
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "connection task dropped without responding",
            ))
        })?;

        let response_data = match result {
            Ok(buf) => {
                tracing::debug!(?correlation_id, bytes = buf.len(), "received response");
                buf
            }
            Err(e) => {
                tracing::warn!(?correlation_id, error = %e, "request failed");
                return Err(e);
            }
        };

        let mut buf = Bytes::from_owner(response_data);
        let resp_header_version = api_key.response_header_version(api_version);
        let _resp_header = ResponseHeader::decode(&mut buf, resp_header_version)?;

        debug_assert_eq!(
            _resp_header.correlation_id, *correlation_id,
            "Correlation ids doesn't match. (Req, Res) pair is wrong"
        );

        let response = Resp::decode(&mut buf, api_version)?;
        Ok(response)
    }

    pub async fn fetch_metadata(&self) -> Result<MetadataResponse> {
        let version = negotiate_version(&self.api_versions, ApiKey::Metadata, 1)?;
        let request = MetadataRequest::default().with_topics(None);
        self.send(ApiKey::Metadata, version, request).await
    }

    pub fn api_versions(&self) -> &[ApiVersion] {
        &self.api_versions
    }

    /// Resolve the wire version to use for `api_key`: picks
    /// `min(desired, broker_max)` after confirming the broker advertises
    /// the API at all before calling [`BrokerClient::send`].
    pub fn negotiate_version(&self, api_key: ApiKey, desired: i16) -> Result<i16> {
        negotiate_version(&self.api_versions, api_key, desired)
    }

    /// Returns `true` if the broker shut down
    pub(crate) fn is_shutdown(&self) -> bool {
        // Acquire pairs with read_task's Release store at the bottom of read_task,
        // matching the ordering send_raw already uses.
        self.shutdown.load(Ordering::Acquire)
    }

    /// Stable per-instance identity assigned at construction.
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Test-only: flip the shutdown flag without actually killing the socket.
    /// Lets unit tests exercise the corpse-eviction path without orchestrating
    /// a real broker disconnect.
    #[cfg(test)]
    pub(crate) fn force_shutdown_for_test(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Run a SASL/PLAIN handshake + authenticate against the broker. Used both for initial
    /// auth and for periodic re-authentication. Returns `Some(session_lifetime)` if the
    /// broker enforces re-auth (`session_lifetime_ms > 0`), otherwise `None`.
    async fn authenticate(&self, auth: &Auth) -> Result<Option<Duration>> {
        let Auth::Plain { username, password } = auth else {
            return Ok(None);
        };

        tracing::info!(username = %username, "starting SASL/PLAIN authentication");

        let has_handshake = self
            .api_versions
            .iter()
            .any(|v| v.api_key == ApiKey::SaslHandshake as i16);
        let has_authenticate = self
            .api_versions
            .iter()
            .any(|v| v.api_key == ApiKey::SaslAuthenticate as i16);
        if !has_handshake || !has_authenticate {
            return Err(Error::AuthenticationError(
                "broker does not support SASL authentication (missing API key 17 or 36)".into(),
            ));
        }

        // SaslHandshake v1
        let handshake_resp: SaslHandshakeResponse = self
            .send(
                ApiKey::SaslHandshake,
                1,
                SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
            )
            .await?;
        if handshake_resp.error_code != 0 {
            return Err(Error::AuthenticationError(format!(
                "SASL handshake failed with error code: {}",
                handshake_resp.error_code
            )));
        }

        // Negotiate SaslAuthenticate version: min(2, broker_max). v1+ is required to receive
        // session_lifetime_ms (KIP-368), so we cap at 2 to avoid asking for fields we don't parse.
        let auth_version = negotiate_version(&self.api_versions, ApiKey::SaslAuthenticate, 2)?;
        tracing::debug!(auth_version, "negotiated SaslAuthenticate version");

        let token = build_plain_token(username, password.expose_secret());
        let auth_resp: SaslAuthenticateResponse = self
            .send(
                ApiKey::SaslAuthenticate,
                auth_version,
                SaslAuthenticateRequest::default().with_auth_bytes(token),
            )
            .await?;
        if auth_resp.error_code != 0 {
            let msg = auth_resp
                .error_message
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("error code: {}", auth_resp.error_code));
            return Err(Error::AuthenticationError(msg));
        }

        let session_lifetime_ms = auth_resp.session_lifetime_ms;
        tracing::info!(
            username = %username,
            session_lifetime_ms,
            "SASL/PLAIN authentication successful"
        );

        let duration = if session_lifetime_ms > 0 {
            Some(Duration::from_millis(session_lifetime_ms as u64))
        } else {
            None
        };

        Ok(duration)
    }
}

/// Find `api_key` in api_versions and returns the `min(desired, broker_max)`
fn negotiate_version(api_versions: &[ApiVersion], api_key: ApiKey, desired: i16) -> Result<i16> {
    let range = api_versions
        .iter()
        .find(|v| v.api_key == api_key as i16)
        .ok_or_else(|| {
            Error::ProtocolError(format!(
                "broker does not support API {:?} (key {})",
                api_key, api_key as i16,
            ))
        })?;
    let version = desired.min(range.max_version);
    if version < range.min_version {
        return Err(Error::ProtocolError(format!(
            "API {:?}: broker supports versions {}..={}, but client needs version {}",
            api_key, range.min_version, range.max_version, desired,
        )));
    }
    Ok(version)
}

/// Build the SASL/PLAIN auth token: \0<username>\0<password>
fn build_plain_token(username: &str, password: &str) -> Bytes {
    let mut token = Vec::with_capacity(1 + username.len() + 1 + password.len());
    token.push(0u8);
    token.extend_from_slice(username.as_bytes());
    token.push(0u8);
    token.extend_from_slice(password.as_bytes());
    Bytes::from(token)
}

/// Compute the re-auth delay: 85–95% of `session_lifetime`, with random jitter.
fn reauth_delay(session_lifetime: Duration) -> Duration {
    let pct = 85 + fastrand::u64(0..=10); // 85–95%
    Duration::from_millis(session_lifetime.as_millis() as u64 * pct / 100)
}

/// Background task: periodically re-authenticate the connection to keep it alive past the
/// broker's `connections.max.reauth.ms` (KIP-368). Exits when the connection dies, when
/// re-auth fails, or when the broker stops returning a session lifetime.
async fn reauth_task(
    client: BrokerClient,
    auth: Auth,
    mut session_lifetime: Duration,
    reauth_shutdown_tx: oneshot::Sender<()>,
) {
    loop {
        let delay = reauth_delay(session_lifetime);
        tracing::debug!(?delay, "re-auth task sleeping");
        tokio::time::sleep(delay).await;

        tracing::info!("re-auth timer fired, starting re-authentication");
        match client.authenticate(&auth).await {
            Ok(Some(new_lifetime)) => {
                tracing::info!(?new_lifetime, "re-authentication successful");
                session_lifetime = new_lifetime;
            }
            Ok(None) => {
                tracing::info!("re-auth returned no session lifetime, stopping reauth task");
                return; // drops sender → read_task sees Err(Closed), ignores it
            }
            Err(e) => {
                tracing::error!(error = %e, "re-authentication failed, shutting down connection");
                let _ = reauth_shutdown_tx.send(()); // signal read_task to tear down
                return;
            }
        }
    }
}

const RECV_MANY_BATCH_COUNT: usize = 32;

/// Write task: pulls `RequestMsg`s from the channel, registers them in `in_flight`, and
/// writes them to the broker. Uses `recv_many` to request_batch queued requests and flush
/// once per request_batch, reducing syscalls under concurrent load. Exits on a write/flush
/// error, when the request channel is closed (BrokerClient dropped), or when the
/// read task signals shutdown.
async fn write_task(
    mut writer: tokio::io::WriteHalf<crate::connection::Stream>,
    mut request_rx: mpsc::Receiver<RequestMsg>,
    in_flight: InFlight,
    mut read_shutdown: oneshot::Receiver<()>,
) {
    let mut request_batch: Vec<RequestMsg> = Vec::with_capacity(RECV_MANY_BATCH_COUNT);
    let mut data_batch = Vec::with_capacity(RECV_MANY_BATCH_COUNT);

    'outer: loop {
        request_batch.clear();
        data_batch.clear();

        let count = tokio::select! {
            biased;
            _ = &mut read_shutdown => {
                tracing::warn!("read task signaled shutdown, exiting write task");
                break;
            }
            // recv_many is cancel-safe: messages already moved into `request_batch`
            // survive cancellation and would be processed on the next iteration.
            count = request_rx.recv_many(&mut request_batch, RECV_MANY_BATCH_COUNT) => count,
        };
        if count == 0 {
            tracing::debug!("request channel closed, exiting write task");
            break;
        }

        tracing::trace!(count, "writing request request_batch to broker");

        {
            let mut q = in_flight.lock().unwrap();
            for req in request_batch.drain(0..count) {
                q.push_back((req.correlation_id, req.response_tx));
                data_batch.push((req.correlation_id, req.data));
            }
        }

        for (correlation_id, data) in data_batch.drain(0..count) {
            if let Err(e) = writer.write_all(&data).await {
                tracing::error!(correlation_id = ?correlation_id, error = %e, "write failed");

                {
                    let mut q = in_flight.lock().unwrap();
                    // The failed entry is always the last one we pushed.
                    if let Some((_, tx)) = q.pop_back() {
                        let _ =
                            tx.send(Err(Error::Io(std::io::Error::new(e.kind(), e.to_string()))));
                    }
                }

                break 'outer;
            }
        }

        if let Err(e) = writer.flush().await {
            tracing::error!(error = %e, "flush failed, exiting write task");
            break;
        }
    }

    // Shut down the write half so the broker closes the connection. The resulting EOF on
    // the read side causes read_task to drain in_flight and exit.
    let _ = writer.shutdown().await;
    tracing::info!("write task exiting");
}

/// Read task: reads framed responses from the broker and dispatches them directly to the
/// waiting caller via the shared `in_flight` map. Drains any remaining in-flight requests
/// on exit.
async fn read_task(
    mut reader: tokio::io::ReadHalf<crate::connection::Stream>,
    in_flight: InFlight,
    max_response_size: usize,
    read_shutdown: oneshot::Sender<()>,
    shutdown: Arc<AtomicBool>,
    reauth_shutdown_rx: oneshot::Receiver<()>,
    pool: BufferPool,
) {
    let mut size_buf = [0u8; 4];
    let mut reauth_shutdown = std::pin::pin!(reauth_shutdown_rx);
    loop {
        let read_result = tokio::select! {
            biased;
            // The sender is either held alive inside `BrokerClient` (no reauth
            // needed — stays pending forever) or owned by `reauth_task` (only
            // sends on failure). Either way this branch only fires on failure.
            _ = &mut reauth_shutdown => {
                tracing::error!("re-authentication failed, shutting down connection");
                break;
            }
            result = reader.read_exact(&mut size_buf) => result,
        };

        if let Err(e) = read_result {
            // UnexpectedEof on a frame boundary means the peer closed cleanly between
            // responses — typically the user dropping the client. Anything else is a
            // genuine surprise (mid-frame disconnect, reset, etc.) and worth a warning.
            if is_connection_closed(&e) {
                tracing::info!(error = %e, "connection closed while waiting for next response");
            } else {
                tracing::warn!(error = %e, "connection lost while reading response size");
            }
            break;
        }

        let response_size = i32::from_be_bytes(size_buf);
        let Ok(response_size) = usize::try_from(response_size) else {
            tracing::error!(response_size, "invalid response size, closing read task");
            break;
        };
        if response_size == 0 || response_size > max_response_size {
            tracing::error!(
                response_size,
                max_response_size,
                "invalid response size, closing read task"
            );
            break;
        }

        if response_size < 4 {
            tracing::error!(response_size, "response too short.");
            break;
        }

        let mut response_buf = pool.get(response_size);
        debug_assert_eq!(response_buf.len(), response_size);

        if let Err(e) = reader.read_exact(&mut response_buf).await {
            // Mid-frame disconnect: we already read the size header, so an EOF here
            // means the broker dropped us partway through a response. Still demote
            // the clean-close case to info to avoid noise on user-initiated shutdown.
            if is_connection_closed(&e) {
                tracing::info!(response_size, error = %e, "connection closed while reading response body");
            } else {
                tracing::warn!(error = %e, "connection lost while reading response body");
            }
            break;
        }

        let correlation_id = match CorrelationId::try_from(response_buf.as_slice()) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error = %e, "Cannot get correlation_id");
                break;
            }
        };
        tracing::trace!(
            ?correlation_id,
            bytes = response_buf.len(),
            "received response from broker"
        );

        let tx = {
            let mut q = in_flight.lock().unwrap();
            match q.front() {
                Some((id, _)) if *id == correlation_id => q.pop_front().map(|(_, tx)| tx),
                Some((id, _)) => {
                    debug_assert_eq!(*id, correlation_id, "Kafka in-order responses guarantee is broken");
                    None
                },
                None => {
                    debug_assert!(false, "Unexpected response. No request is performed");
                    None
                },
            }
        };

        if let Some(tx) = tx {
            let _ = tx.send(Ok(response_buf));
        } else {
            // The queue is out of sync with the broker's response stream.
            // Every subsequent response would also mismatch, so the
            // connection is irrecoverable — tear it down.
            tracing::error!(
                ?correlation_id,
                "in-flight queue desynchronized, closing connection"
            );
            break;
        }
    }

    // Flip the shared flag *before* draining so any caller currently in send_raw
    // sees it as early as possible. This is a best-effort fast path: there is
    // still a TOCTOU window between the flag check in send_raw and the channel
    // send, which is documented at the check site and handled by the
    // dropped-oneshot safety net.
    shutdown.store(true, Ordering::Release);

    drain_in_flight(&in_flight, "read task exited");
    tracing::info!("read task exiting");

    // Wake write_task: dropping this sender resolves the oneshot in its biased
    // `select!`, so it stops pulling new requests off the channel instead of
    // writing them into a connection whose reader is gone.
    drop(read_shutdown);
}

/// Classify an I/O error as "the connection was closed" vs. an unexpected failure.
/// Used to demote shutdown noise from `warn!` to `info!` so users who explicitly
/// drop the client don't see scary log lines for the resulting EOF.
fn is_connection_closed(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
    )
}

fn drain_in_flight(in_flight: &InFlight, message: &str) {
    let mut q = match in_flight.lock() {
        LockResult::Ok(guard) => guard,
        LockResult::Err(err) => err.into_inner(),
    };

    let count = q.len();
    if count > 0 {
        tracing::warn!(count, reason = message, "draining in-flight requests");
    }
    for (_, tx) in q.drain(..) {
        if tx
            .send(Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                message.to_string(),
            ))))
            .is_err()
        {
            // The oneshot receiver was dropped before we could deliver the abort,
            // so the caller has already given up waiting (e.g. its future was
            // cancelled). Nothing to do, but log it so we notice if it becomes common.
            tracing::error!(
                reason = message,
                "failed to notify in-flight caller during drain: receiver dropped"
            );
        }
    }
}
