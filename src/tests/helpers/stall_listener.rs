//! Test-only TCP listener that accepts connections and then does nothing —
//! it never reads, never writes, never closes. Used to drive
//! `Connection::connect` setup-timeout tests where we need a peer that
//! completes the TCP handshake but stalls forever on whatever comes next
//! (e.g. the TLS ClientHello response).
//!
//! The accept loop holds onto every accepted socket so the kernel doesn't
//! reset it; dropping the `StallListener` aborts the loop and releases
//! everything.

use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub struct StallListener {
    pub host: String,
    pub port: u16,
    // Accepted sockets are parked here so they stay open for the lifetime
    // of the listener. Without this, dropping the JoinHandle's stack frame
    // would close them and the client would see a clean RST instead of a
    // hang — defeating the whole point.
    _accepted: Arc<Mutex<Vec<TcpStream>>>,
    accept_task: JoinHandle<()>,
}

impl Drop for StallListener {
    fn drop(&mut self) {
        self.accept_task.abort();
    }
}

pub async fn start_stall_listener() -> StallListener {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind stall listener");
    let local = listener.local_addr().expect("local_addr");
    let accepted: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));

    let accepted_for_task = accepted.clone();
    let accept_task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((sock, _)) => {
                    // Park the socket so it stays open. We never read or
                    // write — the client side will hang waiting for bytes
                    // that will never come.
                    accepted_for_task.lock().unwrap().push(sock);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "stall listener accept failed");
                    return;
                }
            }
        }
    });

    StallListener {
        host: local.ip().to_string(),
        port: local.port(),
        _accepted: accepted,
        accept_task,
    }
}
