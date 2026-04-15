//! Test-only TCP proxy that sits between the kafka-client and a real broker so
//! tests can inject connection-level faults (drops, corrupted bytes, delays,
//! mismatched correlation ids, ...) without forking broker code.
//!
//! The proxy reads length-prefixed Kafka frames in both directions, peeks the
//! header fields needed for fault dispatch (api_key/api_version/correlation_id
//! for requests, correlation_id for responses), and forwards the original
//! bytes untouched on the happy path. Re-encoding is intentionally avoided —
//! a test must be able to observe the *exact* bytes the client wrote.

use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kafka_protocol::messages::ApiKey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Mirrors the client-side cap in `Config::new` / `connection.rs` so the proxy
/// rejects the same malformed frames the client would.
const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

/// Per-direction view passed to fault hooks. `raw` is the frame *payload*
/// (i.e. the bytes after the 4-byte size prefix), so callers that want a
/// typed decode can hand it straight to `kafka-protocol`.
#[allow(dead_code)] // api_version/correlation_id/raw exist for future tests
pub struct RequestView<'a> {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub raw: &'a [u8],
}

#[allow(dead_code)] // correlation_id/raw exist for future tests
pub struct ResponseView<'a> {
    pub correlation_id: i32,
    pub raw: &'a [u8],
}

/// What the proxy should do with a frame instead of forwarding it verbatim.
/// Returned from a `FaultPlan` hook to short-circuit the happy path.
#[derive(Clone)]
#[allow(dead_code)] // variants exist for future tests; not all are used yet
pub enum Fault {
    /// Tear down both halves of the TCP connection immediately.
    DropConnection,
    /// Replace the bytes that would have been forwarded with these. The
    /// caller is responsible for including the 4-byte size prefix — the
    /// proxy writes the buffer as-is.
    Replace(Vec<u8>),
    /// Forward only the first N bytes of the frame (size prefix included)
    /// then drop the connection. Useful for half-frame disconnects.
    Truncate(usize),
    /// Sleep before forwarding the frame as normal.
    Delay(Duration),
    /// Response-side only: rewrite the correlation_id (first 4 bytes of
    /// payload) before forwarding. Lets tests exercise the
    /// in-flight-queue-desync path in `read_task`.
    WrongCorrelationId(i32),
    /// Flip a single byte at `offset` (relative to the full frame including
    /// size prefix) to `value` before forwarding.
    CorruptByte { offset: usize, value: u8 },
}

/// Hooks invoked for each frame in each direction. Returning `Some(Fault)`
/// short-circuits forwarding; returning `None` lets the frame through.
///
/// `Arc<dyn Fn>` rather than a generic so tests can swap plans at runtime
/// without re-parameterizing the `Proxy` type.
#[derive(Clone, Default)]
pub struct FaultPlan {
    pub on_request: Option<Arc<dyn Fn(&RequestView<'_>) -> Option<Fault> + Send + Sync>>,
    pub on_response: Option<Arc<dyn Fn(&ResponseView<'_>) -> Option<Fault> + Send + Sync>>,
}

impl FaultPlan {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Handle returned to tests. Holding it keeps the accept loop alive; dropping
/// it aborts the listener task and releases the bound port.
pub struct ProxyHandle {
    pub host: String,
    pub port: u16,
    plan: Arc<Mutex<FaultPlan>>,
    accept_task: JoinHandle<()>,
}

impl ProxyHandle {
    /// Swap the active fault plan. Takes effect for frames seen after the
    /// swap; in-flight frames keep their original plan.
    #[allow(dead_code)]
    pub fn set_plan(&self, plan: FaultPlan) {
        *self.plan.lock().unwrap() = plan;
    }
}

impl Drop for ProxyHandle {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

/// Bind to `127.0.0.1:0`, spawn the accept loop, and return a handle pointing
/// at the ephemeral port. Each accepted connection opens its own upstream
/// socket to `(upstream_host, upstream_port)`.
pub async fn start(upstream_host: &str, upstream_port: u16, plan: FaultPlan) -> ProxyHandle {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let local = listener.local_addr().expect("local_addr");
    let plan = Arc::new(Mutex::new(plan));

    let upstream = (upstream_host.to_string(), upstream_port);
    let plan_for_task = plan.clone();
    let accept_task = tokio::spawn(async move {
        loop {
            let (client_sock, _) = match listener.accept().await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(error = %e, "proxy accept failed");
                    return;
                }
            };
            let upstream = upstream.clone();
            let plan = plan_for_task.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_conn(client_sock, upstream, plan).await {
                    tracing::debug!(error = %e, "proxy connection closed with error");
                }
            });
        }
    });

    ProxyHandle {
        host: local.ip().to_string(),
        port: local.port(),
        plan,
        accept_task,
    }
}

/// Drive one client<->broker pair until either side closes. We split each
/// socket and run two halves concurrently; the first half to finish aborts
/// the other so a one-sided drop tears the whole connection down.
async fn handle_conn(
    client: TcpStream,
    upstream: (String, u16),
    plan: Arc<Mutex<FaultPlan>>,
) -> io::Result<()> {
    let broker = TcpStream::connect((upstream.0.as_str(), upstream.1)).await?;
    let (mut c_read, mut c_write) = client.into_split();
    let (mut b_read, mut b_write) = broker.into_split();

    let plan_req = plan.clone();
    let mut req_task =
        tokio::spawn(async move { forward_requests(&mut c_read, &mut b_write, plan_req).await });
    let plan_resp = plan.clone();
    let mut resp_task =
        tokio::spawn(async move { forward_responses(&mut b_read, &mut c_write, plan_resp).await });

    // First half to finish wins; abort the other so we don't leak it past
    // the lifetime of the TCP pair.
    tokio::select! {
        r = &mut req_task => {
            resp_task.abort();
            r.unwrap_or(Ok(()))?;
        }
        r = &mut resp_task => {
            req_task.abort();
            r.unwrap_or(Ok(()))?;
        }
    }
    Ok(())
}

async fn forward_requests(
    client_read: &mut tokio::net::tcp::OwnedReadHalf,
    broker_write: &mut tokio::net::tcp::OwnedWriteHalf,
    plan: Arc<Mutex<FaultPlan>>,
) -> io::Result<()> {
    loop {
        let Some(frame) = read_frame(client_read).await? else {
            return Ok(());
        };
        let payload = &frame[4..];

        // Peek the fixed-offset prefix of RequestHeader. These three fields
        // are at the same byte offsets regardless of header flexibility, so
        // we don't need to know header_version to dispatch on api_key.
        let fault =
            if let Some((api_key, api_version, correlation_id)) = peek_request_header(payload) {
                let view = RequestView {
                    api_key,
                    api_version,
                    correlation_id,
                    raw: payload,
                };
                let hook = plan.lock().unwrap().on_request.clone();
                hook.and_then(|f| f(&view))
            } else {
                None
            };

        if !apply_fault(broker_write, &frame, fault).await? {
            return Ok(());
        }
    }
}

async fn forward_responses(
    broker_read: &mut tokio::net::tcp::OwnedReadHalf,
    client_write: &mut tokio::net::tcp::OwnedWriteHalf,
    plan: Arc<Mutex<FaultPlan>>,
) -> io::Result<()> {
    loop {
        let Some(frame) = read_frame(broker_read).await? else {
            return Ok(());
        };
        let payload = &frame[4..];

        // The response correlation_id is always the first 4 bytes of the
        // payload — `broker.rs::read_task` relies on the same property.
        let fault = if payload.len() >= 4 {
            let correlation_id =
                i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let view = ResponseView {
                correlation_id,
                raw: payload,
            };
            let hook = plan.lock().unwrap().on_response.clone();
            hook.and_then(|f| f(&view))
        } else {
            None
        };

        if !apply_fault(client_write, &frame, fault).await? {
            return Ok(());
        }
    }
}

/// Apply `fault` to `frame` and write to `dst`. Returns `Ok(true)` to keep
/// the loop going, `Ok(false)` to tear the half down (the peer half will be
/// aborted by the `select!` in `handle_conn`).
async fn apply_fault(
    dst: &mut tokio::net::tcp::OwnedWriteHalf,
    frame: &[u8],
    fault: Option<Fault>,
) -> io::Result<bool> {
    match fault {
        None => {
            dst.write_all(frame).await?;
            Ok(true)
        }
        Some(Fault::DropConnection) => Ok(false),
        Some(Fault::Replace(bytes)) => {
            dst.write_all(&bytes).await?;
            Ok(true)
        }
        Some(Fault::Truncate(n)) => {
            let n = n.min(frame.len());
            dst.write_all(&frame[..n]).await?;
            Ok(false)
        }
        Some(Fault::Delay(d)) => {
            tokio::time::sleep(d).await;
            dst.write_all(frame).await?;
            Ok(true)
        }
        Some(Fault::WrongCorrelationId(new_id)) => {
            // Rewrite bytes 4..8 of the frame (correlation_id sits at the
            // start of the payload, which itself starts after the 4-byte
            // size prefix).
            let mut tweaked = frame.to_vec();
            if tweaked.len() >= 8 {
                tweaked[4..8].copy_from_slice(&new_id.to_be_bytes());
            }
            dst.write_all(&tweaked).await?;
            Ok(true)
        }
        Some(Fault::CorruptByte { offset, value }) => {
            let mut tweaked = frame.to_vec();
            if offset < tweaked.len() {
                tweaked[offset] = value;
            }
            dst.write_all(&tweaked).await?;
            Ok(true)
        }
    }
}

/// Read one length-prefixed frame. Returns `Ok(None)` on a clean EOF at a
/// frame boundary, mirroring the demotion in `broker.rs::read_task`.
async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let n = i32::from_be_bytes(len_buf);
    if n <= 0 || (n as usize) > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("invalid frame size: {n}"),
        ));
    }
    let mut buf = vec![0u8; 4 + n as usize];
    buf[..4].copy_from_slice(&len_buf);
    r.read_exact(&mut buf[4..]).await?;
    Ok(Some(buf))
}

/// Pull api_key/api_version/correlation_id out of a request payload without
/// touching client_id or tagged fields. Those three are at fixed offsets in
/// every RequestHeader version.
fn peek_request_header(payload: &[u8]) -> Option<(ApiKey, i16, i32)> {
    if payload.len() < 8 {
        return None;
    }
    let api_key = i16::from_be_bytes([payload[0], payload[1]]);
    let api_version = i16::from_be_bytes([payload[2], payload[3]]);
    let correlation_id = i32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
    Some((ApiKey::try_from(api_key).ok()?, api_version, correlation_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::helpers;

    #[tokio::test]
    async fn proxy_forwards_metadata_transparently() {
        let broker = helpers::plaintext_broker().await;
        let proxy = start(&broker.host, broker.port, FaultPlan::new()).await;

        let config = crate::Config::new(&proxy.host, proxy.port);
        let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
            .await
            .unwrap();
        let client = crate::BrokerClient::new(conn, crate::Auth::None)
            .await
            .unwrap();

        let md = client.fetch_metadata().await.unwrap();
        assert!(!md.brokers.is_empty());
    }

    #[tokio::test]
    async fn proxy_drop_connection_fault_surfaces_to_client() {
        let broker = helpers::plaintext_broker().await;

        // Drop only on Metadata so the initial ApiVersions handshake (which
        // BrokerClient::new performs) still succeeds and we can exercise the
        // post-connect failure path.
        let plan = FaultPlan {
            on_request: Some(Arc::new(|view: &RequestView<'_>| {
                if view.api_key == ApiKey::Metadata {
                    Some(Fault::DropConnection)
                } else {
                    None
                }
            })),
            on_response: None,
        };
        let proxy = start(&broker.host, broker.port, plan).await;

        let config = crate::Config::new(&proxy.host, proxy.port);
        let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
            .await
            .unwrap();
        let client = crate::BrokerClient::new(conn, crate::Auth::None)
            .await
            .unwrap();

        let err = client.fetch_metadata().await.unwrap_err();
        match err {
            crate::Error::Io(e) => {
                assert!(
                    matches!(
                        e.kind(),
                        ErrorKind::ConnectionAborted
                            | ErrorKind::UnexpectedEof
                            | ErrorKind::BrokenPipe
                            | ErrorKind::ConnectionReset
                    ),
                    "unexpected io error kind: {:?}",
                    e.kind()
                );
            }
            other => panic!("expected Io error, got {other:?}"),
        }
    }
}
