//! Tests for fatal-error handling in the per-broker dialer
//! (`perform_backoff_retry`). When a dial attempt fails with a fatal error
//! (authentication, protocol, config), the dialer must give up immediately
//! and wake all parked callers instead of looping forever.

use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::BrokerId;

use super::helpers;
use super::helpers::proxy::{Fault, FaultPlan};

/// Inject a garbage ApiVersions response so the handshake inside
/// `BrokerClient::new` hits a decode error → `Error::Protocol` → fatal.
/// The 8-byte payload is a valid frame header (size=4, correlation_id=0)
/// with no body — too short for `ApiVersionsResponse::decode`.
fn fatal_response_plan() -> FaultPlan {
    FaultPlan {
        on_request: None,
        on_response: Some(Arc::new(move |_view| {
            // Replace every broker response with a minimal valid-framed
            // but undecipherable payload. correlation_id = 0 matches the
            // first request the handshake sends.
            let mut buf = Vec::with_capacity(8);
            buf.extend_from_slice(&4i32.to_be_bytes()); // size prefix: 4 bytes of payload
            buf.extend_from_slice(&0i32.to_be_bytes()); // correlation_id = 0
            Some(Fault::Replace(buf))
        })),
    }
}

const SYNTH_ID: BrokerId = BrokerId(8888);

/// Bootstrap against a real broker, then point a synthetic broker at the
/// proxy. Mirrors `reconnect_backoff::client_through_proxy`.
async fn client_through_proxy(proxy: &helpers::proxy::ProxyHandle) -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)
        .with_reconnect_backoff(Duration::from_millis(50))
        .with_reconnect_backoff_max(Duration::from_millis(200))];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    let controller = client.controller_id();
    client.replace_metadata_for_test(
        controller,
        vec![
            (controller, broker.host.clone(), broker.port),
            (SYNTH_ID, proxy.host.clone(), proxy.port),
        ],
        vec![],
    );
    client
}

// ---------------------------------------------------------------------------
// Fatal dial error wakes callers immediately.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn fatal_dial_error_wakes_callers() {
    let broker = helpers::plaintext_broker().await;
    let proxy = helpers::proxy::start(&broker.host, broker.port, fatal_response_plan()).await;
    let client = client_through_proxy(&proxy).await;

    // The dialer for SYNTH_ID should hit a fatal Protocol error on the
    // first attempt and give up. The caller must see an error promptly
    // (well under the 5 s timeout guard).
    let result = tokio::time::timeout(Duration::from_secs(5), client.broker(SYNTH_ID)).await;

    let broker_result = result.expect("broker() should not hang on fatal dial error");
    assert!(
        broker_result.is_err(),
        "expected Err from broker() after fatal dial, got Ok"
    );

    // Verify the dialer actually hit the fatal-error code path, not some
    // other failure mode (timeout, abort, connection drop).
    assert!(
        logs_contain("fatal broker dial error; giving up"),
        "dialer did not log the fatal-error message — the test may not be exercising the right code path"
    );

    client.close();
}

// ---------------------------------------------------------------------------
// Multiple concurrent callers all wake on a fatal dial error.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn fatal_dial_error_wakes_all_concurrent_callers() {
    let broker = helpers::plaintext_broker().await;
    let proxy = helpers::proxy::start(&broker.host, broker.port, fatal_response_plan()).await;
    let client = client_through_proxy(&proxy).await;

    let mut handles = Vec::with_capacity(5);
    for _ in 0..5 {
        let c = client.clone();
        handles.push(tokio::spawn(async move { c.broker(SYNTH_ID).await }));
    }

    for (i, h) in handles.into_iter().enumerate() {
        let r = tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .unwrap_or_else(|_| panic!("waiter {i} hung on fatal dial error"))
            .expect("join");
        assert!(
            r.is_err(),
            "waiter {i}: expected Err after fatal dial, got Ok"
        );
    }

    assert!(
        logs_contain("fatal broker dial error; giving up"),
        "dialer did not log the fatal-error message — the test may not be exercising the right code path"
    );

    client.close();
}

// ---------------------------------------------------------------------------
// After a fatal dial, a new broker() call can respawn a fresh dialer.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn slot_removed_after_fatal_dial_allows_respawn() {
    let broker = helpers::plaintext_broker().await;
    let proxy = helpers::proxy::start(&broker.host, broker.port, fatal_response_plan()).await;
    let client = client_through_proxy(&proxy).await;

    // First attempt: fatal error removes the slot.
    let r = tokio::time::timeout(Duration::from_secs(5), client.broker(SYNTH_ID)).await;
    assert!(r.expect("should not hang").is_err());
    assert!(
        logs_contain("fatal broker dial error; giving up"),
        "dialer did not log the fatal-error message — the test may not be exercising the right code path"
    );

    // Swap to a healthy proxy so the next dialer succeeds.
    proxy.set_plan(FaultPlan::new());

    // Second attempt: a fresh dialer should be spawned and succeed.
    let r = tokio::time::timeout(Duration::from_secs(5), client.broker(SYNTH_ID)).await;
    let broker_client = r
        .expect("respawned dialer should not hang")
        .expect("respawned dialer should succeed with healthy proxy");

    // Verify we got a working client.
    assert!(!broker_client.is_shutdown());

    client.close();
}
