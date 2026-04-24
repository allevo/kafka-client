use std::collections::VecDeque;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, LockResult, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::{BufMut, Bytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};

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

/// Function that, given the broker-advertised session lifetime, returns
/// when the next re-auth attempt should fire. See [`default_reauth_delay`].
pub type ReauthDelayFn = Arc<dyn Fn(Duration) -> Duration + Send + Sync>;

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

type InFlight = Arc<
    Mutex<
        VecDeque<(
            CorrelationId,
            oneshot::Sender<Result<zeropool::PooledBuffer>>,
        )>,
    >,
>;

#[derive(Clone)]
pub struct BrokerClient {
    inner: Arc<BrokerClientInner>,
}

struct BrokerClientInner {
    /// Stable per-instance identity assigned at construction.
    #[cfg(test)]
    id: u64,
    request_tx: mpsc::Sender<RequestMsg>,
    next_correlation_id: AtomicI32,
    pool: BufferPool,
    api_versions: Box<[ApiVersion]>,
    // Flipped by `read_task` the moment it decides to exit, so `send_request`
    // can reject new requests immediately instead of letting them sit in the mpsc
    // channel until `write_task` notices the shutdown signal and drops `request_rx`.
    shutdown: Arc<AtomicBool>,
    // User-initiated shutdown signal. `read_task` selects on `close.notified()`;
    // waking it tears down the connection through the existing read→write
    // shutdown chain.
    close: Arc<Notify>,
    // The reauth_task-shutdown notify.
    reauth_close: Arc<Notify>,
    // Prevent shut down for idle during re-auth.
    reauth_pending: Arc<AtomicBool>,
    // Holds the reauth-shutdown sender alive when no reauth task is running.
    // Dropped alongside this struct, which closes the oneshot and is a no-op
    // for `read_task` (the branch is dormant).
    _reauth_shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl Drop for BrokerClientInner {
    fn drop(&mut self) {
        // Fired when the last external `BrokerClient` clone has been dropped.
        self.shutdown.store(true, Ordering::Release);
        self.close.notify_one();
        self.reauth_close.notify_one();
    }
}

const CLIENT_ID: &str = "kafka-client";

impl BrokerClient {
    /// Build a `BrokerClient` from an established `Connection`, performing initial SASL auth
    /// (if requested) and spawning a background re-auth task when the broker reports a
    /// non-zero `session_lifetime_ms`.
    ///
    /// `reauth_delay` is used to calculate the delay for next auth.
    pub async fn new(
        mut connection: Connection,
        auth: Auth,
        reauth_delay: Option<ReauthDelayFn>,
    ) -> Result<Self> {
        let mut correlation_id: i32 = 0;

        let api_versions = connection.fetch_api_versions(&mut correlation_id).await?;

        let pool = BufferPool::new();
        let max_response_size = connection.max_response_size;

        let connections_max_idle = jitter_connection_max_idle(connection.connections_max_idle);

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
        let close = Arc::new(Notify::new());
        let reauth_close = Arc::new(Notify::new());
        let reauth_pending = Arc::new(AtomicBool::new(false));

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
            close.clone(),
            pool.clone(),
            connections_max_idle,
            reauth_pending.clone(),
        ));

        let client = BrokerClient {
            inner: Arc::new(BrokerClientInner {
                #[cfg(test)]
                id: fastrand::u64(..),
                request_tx,
                pool,
                next_correlation_id: AtomicI32::new(correlation_id + 1),
                api_versions: api_versions.into(),
                shutdown,
                close,
                reauth_close,
                reauth_pending,
                // Keep the sender alive so the oneshot in read_task stays pending.
                // Moved into reauth_task below if reauth is needed.
                _reauth_shutdown_tx: Mutex::new(None),
            }),
        };

        let session_lifetime = client.authenticate(&auth).await?;

        // KIP-368: brokers with connections.max.reauth.ms > 0 will kill connections that
        // don't re-authenticate before the session expires. Spawn a background task that
        // sleeps and re-authenticates periodically.
        if let Some(lifetime) = session_lifetime {
            tracing::info!(?lifetime, "spawning re-auth task");

            // NB: Weak so, it doesn't count as strong reference
            let reauth_close = client.inner.reauth_close.clone();
            let delay_fn: ReauthDelayFn =
                reauth_delay.unwrap_or_else(|| Arc::new(default_reauth_delay));
            tokio::spawn(reauth_task(
                Arc::downgrade(&client.inner),
                reauth_close,
                auth,
                lifetime,
                reauth_shutdown_tx,
                delay_fn,
            ));
        } else {
            tracing::info!("Broker doesn't require re-auth task");
            // Store reauth_shutdown_tx, so the `read_task` can poll it even if it will never be resolved
            // This make the code easier at a little cost of a `tokio::sync::oneshot::Receiver::poll`
            let mut guard = client.inner._reauth_shutdown_tx.lock().unwrap();
            *guard = Some(reauth_shutdown_tx);
        }

        Ok(client)
    }

    /// Queue a typed Kafka request and return a handle for its response.
    pub async fn send_request<Req, Resp>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request: &Req,
    ) -> Result<ResponseFuture<Resp>>
    where
        Req: Encodable + HeaderVersion + Send + 'static,
        Resp: Decodable + HeaderVersion,
    {
        // Fast-path rejection once `read_task` has decided to exit.
        // NB: a small unavoidable TOCTOU window is still possibile.
        //     But it is temporary.
        // So this flag is a best-effort fast path; In the TOCTOU window
        // there's a safety net.
        if self.inner.shutdown.load(Ordering::Acquire) {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "broker connection closed",
            )));
        }

        // correlation_id is a client side parameter. There is not guarantees of the values
        // No lock is needed.
        let correlation_id = CorrelationId(
            self.inner
                .next_correlation_id
                .fetch_add(1, Ordering::Relaxed),
        );

        // Serialize here (not in `write_task`) so encoding cost is paid on
        // the caller's task rather than serialised through the single
        // writer; decoding lives in `ResponseFuture::poll` for the same
        // reason.

        let header = RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(*correlation_id)
            .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));

        let header_version = Req::header_version(api_version);
        let size = header.compute_size(header_version)? + request.compute_size(api_version)?;
        let mut buf = self.inner.pool.get(4 + size);
        debug_assert_eq!(buf.len(), 4 + size);
        // We want to start from position 0, discarding dirty (and old) values
        buf.clear();
        let size = i32::try_from(size).map_err(|_| {
            Error::Protocol(format!("request too large for i32 frame size: {size}"))
        })?;
        buf.put_i32(size);
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

        self.inner.request_tx.send(request_msg).await.map_err(|_| {
            tracing::error!("connection task has shut down");
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "connection task has shut down",
            ))
        })?;

        Ok(ResponseFuture {
            correlation_id,
            response_rx,
            api_key,
            api_version,
            _marker: PhantomData,
        })
    }

    pub async fn fetch_metadata(&self) -> Result<MetadataResponse> {
        let version = negotiate_version(&self.inner.api_versions, ApiKey::Metadata, 1)?;
        let request = MetadataRequest::default().with_topics(None);
        self.send_request(ApiKey::Metadata, version, &request)
            .await?
            .await
    }

    pub fn api_versions(&self) -> &[ApiVersion] {
        &self.inner.api_versions
    }

    /// Resolve the wire version to use for `api_key`: picks
    /// `min(desired, broker_max)` after confirming the broker advertises
    /// the API at all before calling [`BrokerClient::send_request`].
    pub fn negotiate_version(&self, api_key: ApiKey, desired: i16) -> Result<i16> {
        negotiate_version(&self.inner.api_versions, api_key, desired)
    }

    /// Signal this broker connection to tear down. After calling this, the
    /// background read/write tasks will exit asynchronously and any pending
    /// in-flight requests will complete with `ConnectionAborted`. Idempotent:
    /// safe to call on an already-shut-down broker.
    pub fn shutdown(&self) {
        self.inner.shutdown.store(true, Ordering::Release);

        self.inner.close.notify_one();
        self.inner.reauth_close.notify_one();
    }

    /// Returns `true` if the broker shut down
    pub(crate) fn is_shutdown(&self) -> bool {
        // Acquire pairs with read_task's Release store at the bottom of read_task,
        // matching the ordering send_request already uses.
        self.inner.shutdown.load(Ordering::Acquire)
    }

    /// Stable per-instance identity assigned at construction.
    #[cfg(test)]
    pub(crate) fn id(&self) -> u64 {
        self.inner.id
    }

    /// Test-only: flip the shutdown flag without actually killing the socket.
    /// Lets unit tests exercise the corpse-eviction path without orchestrating
    /// a real broker disconnect.
    #[cfg(test)]
    pub(crate) fn force_shutdown_for_test(&self) {
        self.inner.shutdown.store(true, Ordering::Release);
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
            .inner
            .api_versions
            .iter()
            .any(|v| v.api_key == ApiKey::SaslHandshake as i16);
        let has_authenticate = self
            .inner
            .api_versions
            .iter()
            .any(|v| v.api_key == ApiKey::SaslAuthenticate as i16);
        if !has_handshake || !has_authenticate {
            return Err(Error::Authentication(
                "broker does not support SASL authentication (missing API key 17 or 36)".into(),
            ));
        }

        // SaslHandshake v1
        let handshake_resp: SaslHandshakeResponse = self
            .send_request(
                ApiKey::SaslHandshake,
                1,
                &SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN")),
            )
            .await?
            .await?;
        if handshake_resp.error_code != 0 {
            return Err(Error::Authentication(format!(
                "SASL handshake failed with error code: {}",
                handshake_resp.error_code
            )));
        }

        // Negotiate SaslAuthenticate version: min(2, broker_max). v1+ is required to receive
        // session_lifetime_ms (KIP-368), so we cap at 2 to avoid asking for fields we don't parse.
        let auth_version =
            negotiate_version(&self.inner.api_versions, ApiKey::SaslAuthenticate, 2)?;
        tracing::debug!(auth_version, "negotiated SaslAuthenticate version");

        let token = build_plain_token(username, password.expose_secret());
        let auth_resp: SaslAuthenticateResponse = self
            .send_request(
                ApiKey::SaslAuthenticate,
                auth_version,
                &SaslAuthenticateRequest::default().with_auth_bytes(token),
            )
            .await?
            .await?;
        if auth_resp.error_code != 0 {
            let msg = auth_resp
                .error_message
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("error code: {}", auth_resp.error_code));
            return Err(Error::Authentication(msg));
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

/// Handle for an in-flight request whose response has not yet arrived.
///
/// Await it to receive the decoded response, or drop it to discard the response.
/// Dropping is safe: the correlation queue stays aligned.
pub struct ResponseFuture<Resp> {
    correlation_id: CorrelationId,
    response_rx: oneshot::Receiver<Result<zeropool::PooledBuffer>>,
    api_key: ApiKey,
    api_version: i16,
    // `fn() -> Resp` keeps the future Send/Sync regardless of Resp.
    _marker: PhantomData<fn() -> Resp>,
}

impl<Resp> ResponseFuture<Resp>
where
    Resp: Decodable + HeaderVersion,
{
    fn decode(&self, response_data: zeropool::PooledBuffer) -> Result<Resp> {
        let mut buf = Bytes::from_owner(response_data);
        let resp_header_version = self.api_key.response_header_version(self.api_version);
        let _resp_header = ResponseHeader::decode(&mut buf, resp_header_version)?;

        debug_assert_eq!(
            _resp_header.correlation_id, *self.correlation_id,
            "Correlation ids doesn't match. (Req, Res) pair is wrong"
        );

        let response = Resp::decode(&mut buf, self.api_version)?;
        Ok(response)
    }
}

impl<Resp> Future for ResponseFuture<Resp>
where
    Resp: Decodable + HeaderVersion,
{
    type Output = Result<Resp>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match Pin::new(&mut this.response_rx).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => {
                tracing::error!(
                    correlation_id = ?this.correlation_id,
                    "connection task dropped without responding"
                );
                Poll::Ready(Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    "connection task dropped without responding",
                ))))
            }
            Poll::Ready(Ok(Err(e))) => {
                tracing::warn!(
                    correlation_id = ?this.correlation_id,
                    error = %e,
                    "request failed"
                );
                Poll::Ready(Err(e))
            }
            Poll::Ready(Ok(Ok(buf))) => {
                tracing::debug!(
                    correlation_id = ?this.correlation_id,
                    bytes = buf.len(),
                    "received response"
                );
                Poll::Ready(this.decode(buf))
            }
        }
    }
}

/// Find `api_key` in api_versions and returns the `min(desired, broker_max)`
fn negotiate_version(api_versions: &[ApiVersion], api_key: ApiKey, desired: i16) -> Result<i16> {
    let range = api_versions
        .iter()
        .find(|v| v.api_key == api_key as i16)
        .ok_or_else(|| {
            Error::Protocol(format!(
                "broker does not support API {:?} (key {})",
                api_key, api_key as i16,
            ))
        })?;
    let version = desired.min(range.max_version);
    if version < range.min_version {
        return Err(Error::Protocol(format!(
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

/// Default re-auth delay: 75–85% of `session_lifetime`, with random jitter.
///
/// KIP-368: re-authenticate before the broker enforces
/// `connections.max.reauth.ms`. The 15–25% margin at the default absorbs
/// scheduler jitter plus the SASL handshake's own latency.
pub fn default_reauth_delay(session_lifetime: Duration) -> Duration {
    let pct = 75 + fastrand::u32(0..=10); // 75–85%
    session_lifetime * pct / 100
}

/// Background task: periodically re-authenticate the connection to keep it alive past the
/// broker's `connections.max.reauth.ms` (KIP-368). Exits when the connection dies, when
/// re-auth fails, when the broker stops returning a session lifetime, or when the user
/// drops the last external `BrokerClient` handle (`Weak::upgrade` will return None).
async fn reauth_task(
    inner: Weak<BrokerClientInner>,
    reauth_close: Arc<Notify>,
    auth: Auth,
    mut session_lifetime: Duration,
    reauth_shutdown_tx: oneshot::Sender<()>,
    delay_fn: ReauthDelayFn,
) {
    loop {
        let delay = delay_fn(session_lifetime);
        tracing::debug!(?delay, "re-auth task sleeping");
        // Force to stop the task when a shutdown is requested instead of
        // waiting the next cicle.
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = reauth_close.notified() => {
                tracing::info!("reauth task observed shutdown, exiting");
                return;
            }
        }

        // None means inner is freed.
        let Some(strong) = inner.upgrade() else {
            tracing::info!("reauth task: client dropped, exiting");
            return;
        };
        let client = BrokerClient { inner: strong };

        tracing::info!("re-auth timer fired, starting re-authentication");
        // Signal read_task that a re-auth is imminent so it doesn't idle-close
        // the connection before our requests hit in_flight.
        client.inner.reauth_pending.store(true, Ordering::Release);
        let result = client.authenticate(&auth).await;
        client.inner.reauth_pending.store(false, Ordering::Release);
        // Release our strong ref *before* the next sleep so dropping the
        // user's last external handle in the meantime triggers
        // `BrokerClientInner::drop`, whose `reauth_close.notify_one()` then
        // unblocks the next `select!` iteration.
        drop(client);
        match result {
            Ok(Some(new_lifetime)) => {
                tracing::info!(?new_lifetime, "re-authentication successful");
                session_lifetime = new_lifetime;
            }
            Ok(None) => {
                tracing::info!("re-auth returned no session lifetime, stopping reauth task");
                // Broker no longer enforces KIP-368 on this session. Park the
                // sender back on the inner so read_task's `reauth_shutdown`
                // arm stays pending forever. Mirrors the init-time no-reauth path
                // at `BrokerClient::new`.
                if let Some(inner) = inner.upgrade() {
                    *inner._reauth_shutdown_tx.lock().unwrap() = Some(reauth_shutdown_tx);
                }
                break;
            }
            Err(e) => {
                tracing::error!(error = %e, "re-authentication failed, shutting down connection");
                let _ = reauth_shutdown_tx.send(()); // signal read_task to tear down
                break;
            }
        }
    }

    tracing::warn!("re-authentication loop is end");
}

const RECV_MANY_BATCH_COUNT: usize = 32;

// Buffer capacity to keep maximum RECV_MANY_BATCH_COUNT batch in memory
// before flushing.
const WRITE_BUFFER_CAPACITY: usize = 128 * 1024;

/// Write task: pulls `RequestMsg`s from the channel, registers them in `in_flight`, and
/// writes them to the broker. Uses `recv_many` to request_batch queued requests and flush
/// once per request_batch, reducing syscalls under concurrent load. Exits on a write/flush
/// error, when the request channel is closed (BrokerClient dropped), or when the
/// read task signals shutdown.
async fn write_task(
    writer: tokio::io::WriteHalf<crate::connection::Stream>,
    mut request_rx: mpsc::Receiver<RequestMsg>,
    in_flight: InFlight,
    mut read_shutdown: oneshot::Receiver<()>,
) {
    // This wrap avoids one syscall per write.
    // NB1: Large Produce/Fetch payloads bypass the buffer via BufWriter's large-write fast path.
    // NB2: We disabled Nagle algo with TCP_NODELAY flag.
    //      This means the flush on buffer forces the write
    let mut writer = tokio::io::BufWriter::with_capacity(WRITE_BUFFER_CAPACITY, writer);
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

        for (i, (correlation_id, data)) in data_batch.drain(0..count).enumerate() {
            if let Err(e) = writer.write_all(&data).await {
                tracing::error!(correlation_id = ?correlation_id, error = %e, "write failed");

                // Pop the failed entry and every entry after it (never written).
                // They sit at the tail of the queue: count - i entries total.
                {
                    let mut q = in_flight.lock().unwrap();
                    for _ in 0..count - i {
                        if let Some((_, tx)) = q.pop_back() {
                            let _ = tx
                                .send(Err(Error::Io(std::io::Error::new(e.kind(), e.to_string()))));
                        }
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
#[allow(clippy::too_many_arguments)]
async fn read_task(
    mut reader: tokio::io::ReadHalf<crate::connection::Stream>,
    in_flight: InFlight,
    max_response_size: usize,
    read_shutdown: oneshot::Sender<()>,
    shutdown: Arc<AtomicBool>,
    reauth_shutdown_rx: oneshot::Receiver<()>,
    close: Arc<Notify>,
    pool: BufferPool,
    connections_max_idle: Option<Duration>,
    reauth_pending: Arc<AtomicBool>,
) {
    let mut size_buf = [0u8; 4];
    // Persistent offset into `size_buf` across loop iterations. The read arm
    // of the `select!` below uses `read` (cancel-safe) rather than `read_exact`
    // (not cancel-safe), so a partial header survives when another arm wins —
    // notably the idle-timer `continue` path with in-flight activity.
    let mut size_bytes_read = 0usize;
    let mut reauth_shutdown = std::pin::pin!(reauth_shutdown_rx);
    let mut last_activity = tokio::time::Instant::now();

    loop {
        let read_result = tokio::select! {
            biased;
            // Fires on `Ok(())` when `reauth_task` signals auth failure, or on
            // `Err(RecvError)` when the sender is dropped — which happens during
            // user-initiated shutdown (`BrokerClientInner::drop`). Because the
            // select is `biased`, this arm wins over `close.notified()` when both
            // are simultaneously ready on shutdown, so disambiguate via the
            // `shutdown` flag to avoid a spurious ERROR on clean close.
            _ = &mut reauth_shutdown => {
                if shutdown.load(Ordering::Acquire) {
                    tracing::info!("reauth sender dropped during client shutdown");
                } else {
                    tracing::error!("re-authentication failed, shutting down connection");
                }
                break;
            }
            // User-initiated shutdown via `BrokerClient::shutdown()`.
            // Breaking here drops `read_shutdown` and
            // drains in_flight, which also tears down `write_task`.
            _ = close.notified() => {
                tracing::info!("client requested shutdown");
                break;
            }
            // Idle-close arm. When the feature is disabled we park on
            // `pending()` forever, which `select!` just ignores.
            _ = async {
                match connections_max_idle {
                    Some(connections_max_idle) => tokio::time::sleep_until(last_activity + connections_max_idle).await,
                    None => std::future::pending::<()>().await,
                }
            } => {
                // If any request is still pending or a re-auth is about to
                // start, reset the counter so we don't tear down the connection.
                // NB: SASL re-auth exchange counts as in-flight activity.
                if !in_flight.lock().unwrap().is_empty()
                    || reauth_pending.load(Ordering::Acquire)
                {
                    last_activity = tokio::time::Instant::now();
                    continue;
                }
                tracing::warn!(
                    idle = ?last_activity.elapsed(),
                    connections_max_idle = ?connections_max_idle,
                    "connections.max.idle exceeded, closing broker connection"
                );
                break;
            }
            // `read` is cancel-safe (unlike `read_exact`): if another arm wins,
            // no bytes have been consumed from the stream. We assemble the
            // 4-byte size header across iterations via `size_bytes_read`.
            result = reader.read(&mut size_buf[size_bytes_read..]) => result,
        };

        let n = match read_result {
            Ok(n) => n,
            Err(e) => {
                if is_connection_closed(&e) {
                    tracing::info!(error = %e, "connection closed while waiting for next response");
                } else {
                    tracing::warn!(error = %e, "connection lost while reading response size");
                }
                break;
            }
        };
        if n == 0 {
            // EOF. Clean if we're at a frame boundary; mid-header otherwise.
            if size_bytes_read == 0 {
                tracing::info!("connection closed while waiting for next response");
            } else {
                tracing::warn!(
                    size_bytes_read,
                    "connection closed mid-frame while reading response size"
                );
            }
            break;
        }
        size_bytes_read += n;
        if size_bytes_read < 4 {
            // Partial size header; loop to read the remainder.
            continue;
        }

        let response_size = i32::from_be_bytes(size_buf);
        size_bytes_read = 0;
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

        // Bump only after a full frame lands so the rule stays simple:
        // "a response arrived".
        last_activity = tokio::time::Instant::now();

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
                    tracing::error!(
                        expected = ?id,
                        actual = ?correlation_id,
                        "out-of-order response: Kafka in-order guarantee is broken"
                    );
                    debug_assert_eq!(
                        *id, correlation_id,
                        "Kafka in-order responses guarantee is broken"
                    );
                    None
                }
                None => {
                    tracing::error!(
                        ?correlation_id,
                        "unexpected response with no in-flight requests"
                    );
                    debug_assert!(false, "Unexpected response. No request is performed");
                    None
                }
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

    // Flip the shared flag *before* draining so any caller currently in
    // send_request sees it as early as possible. This is a best-effort fast
    // path: there is still a TOCTOU window between the flag check in
    // send_request and the channel send, which is documented at the check
    // site and handled by the dropped-oneshot safety net.
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

/// per-broker jitter: subtract up to 2s only when the
/// limit is at least 4s, so we never jitter below 2s. Prevents a cluster
/// of brokers that went idle in lockstep from all disconnecting at the
/// same instant.
fn jitter_connection_max_idle(conf: Option<Duration>) -> Option<Duration> {
    conf.map(|d| {
        if d >= Duration::from_secs(4) {
            d - Duration::from_millis(fastrand::u64(0..2000))
        } else {
            d
        }
    })
}
