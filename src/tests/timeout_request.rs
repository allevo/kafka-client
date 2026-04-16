//! Tests for the per-request budget and the retry loop in `Client::send`
//! (TIMEOUT.md §2.1 + §3 / REQ_TIMEOUT_UPDATED.md).
//!
//! Setup pattern mirrors `reconnect_backoff.rs`: bootstrap against the
//! real plaintext broker, then `replace_metadata_for_test` publishes a
//! synthetic broker id whose host/port points at a proxy or stall
//! listener. `Client::send(NodeTarget::Broker(SYNTH_ID), ...)` then
//! funnels the attempt through the fault injector while the real
//! bootstrap broker stays around for `any_broker()` / `refresh_metadata()`.

use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use kafka_protocol::messages::{ApiKey, BrokerId, MetadataRequest};

use super::helpers;
use super::helpers::proxy::{Fault, FaultPlan, ProxyHandle, RequestView, ResponseView};
use super::helpers::stall_listener::start_stall_listener;

const SYNTH_ID: BrokerId = BrokerId(9999);

/// Bootstrap a `Client` against the real broker and publish `SYNTH_ID`
/// pointing at the given (host, port). Tunables are explicit because
/// Phase 1 tests want `retries = 0` while Phase 2 tests want the loop
/// active.
async fn client_with_synthetic_full(
    host: &str,
    port: u16,
    request_timeout: Duration,
    api_timeout: Duration,
    retries: u32,
) -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)
        .with_request_timeout(request_timeout)
        .with_api_timeout(api_timeout)
        .with_retries(retries)
        .with_retry_backoff(Duration::from_millis(10))
        .with_retry_backoff_max(Duration::from_millis(50))];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    let controller = client.controller_id();
    client.replace_metadata_for_test(
        controller,
        vec![
            (controller, broker.host.clone(), broker.port),
            (SYNTH_ID, host.to_string(), port),
        ],
    );
    client
}

/// Shorthand for Phase 1 tests: `retries = 0`.
async fn client_with_synthetic(host: &str, port: u16, request_timeout: Duration) -> crate::Client {
    // api_timeout must be at least request_timeout for the inner clamp
    // to land on request_timeout rather than on the api cap.
    client_with_synthetic_full(host, port, request_timeout, request_timeout * 4, 0).await
}

fn metadata_request_build(_version: i16) -> MetadataRequest {
    MetadataRequest::default().with_topics(None)
}

/// Wire-phase stall: the proxy lets the Metadata *request* through but
/// delays the *response* past the budget. The first `send` must return
/// `RequestTimeout` roughly within the budget, the underlying broker
/// slot must end up torn down, and a subsequent `send` must redial and
/// succeed (exercises the `Client::broker` self-heal fast path).
#[tokio::test]
#[tracing_test::traced_test]
async fn request_timeout_wire_phase_stall() {
    let upstream = helpers::plaintext_broker().await;

    // Count Metadata responses. Delay the first one past the budget;
    // forward every subsequent response untouched so the second `send`
    // can succeed.
    let delayed = Arc::new(AtomicUsize::new(0));
    let delayed_hook = delayed.clone();
    let plan = FaultPlan {
        on_request: None,
        on_response: Some(Arc::new(move |_view: &ResponseView<'_>| {
            // Delay the first response only. We can't filter by api_key
            // on responses (proxy view has no header), so we just delay
            // the very first response we see on the connection — in
            // this test that is the ApiVersions response... which would
            // hang handshake. So gate on a counter bumped only after we
            // let the handshake through.
            if delayed_hook.load(Ordering::SeqCst) == 0 {
                None
            } else if delayed_hook.fetch_sub(1, Ordering::SeqCst) > 0 {
                Some(Fault::Delay(Duration::from_secs(30)))
            } else {
                None
            }
        })),
    };
    let proxy = helpers::proxy::start(&upstream.host, upstream.port, plan).await;

    let request_timeout = Duration::from_millis(400);
    let client = client_with_synthetic(&proxy.host, proxy.port, request_timeout).await;

    // First, pre-dial the synthetic broker so the handshake (ApiVersions
    // + initial Metadata inside BrokerClient::new) completes cleanly.
    let _ = client
        .broker(SYNTH_ID)
        .await
        .expect("pre-dial synthetic broker");

    // Arm the fault: the *next* Metadata response on this connection
    // gets delayed past the budget.
    delayed.store(1, Ordering::SeqCst);

    let started = Instant::now();
    let err = client
        .send::<_, kafka_protocol::messages::MetadataResponse>(
            crate::NodeTarget::Broker(SYNTH_ID),
            ApiKey::Metadata,
            1,
            metadata_request_build,
        )
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    match err {
        crate::Error::RequestTimeout(_) => {}
        other => panic!("expected RequestTimeout, got {other:?}"),
    }
    assert!(
        elapsed >= request_timeout && elapsed < request_timeout * 4,
        "wire-phase timeout fired at unexpected time: {elapsed:?}"
    );

    // Subsequent send self-heals: the stale `Slot::Resolved` is shut
    // down, so `Client::broker` respawns a dialer and the new
    // connection (proxy plan no longer delays) completes normally.
    // Budget the second call generously.
    tokio::time::timeout(
        Duration::from_secs(5),
        client.send::<_, kafka_protocol::messages::MetadataResponse>(
            crate::NodeTarget::Broker(SYNTH_ID),
            ApiKey::Metadata,
            1,
            metadata_request_build,
        ),
    )
    .await
    .expect("second send must not hang")
    .expect("second send must succeed after self-heal");
}

/// Acquisition-phase stall: the synthetic broker's address points at a
/// stall listener. TCP accept succeeds but the broker never replies to
/// ApiVersions, so the dialer task inside `BrokerClient::new` hangs on
/// the handshake — every caller parked on `broker(id)`'s oneshot stays
/// pending forever. This is the regression guard for review point #1:
/// without the acquisition-side `timeout_at`, `Client::send` would hang
/// indefinitely on a parked dialer.
#[tokio::test]
#[tracing_test::traced_test]
async fn request_timeout_acquisition_phase_stall() {
    let listener = start_stall_listener().await;

    let request_timeout = Duration::from_millis(400);
    let client = client_with_synthetic(&listener.host, listener.port, request_timeout).await;

    let started = Instant::now();
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        client.send::<_, kafka_protocol::messages::MetadataResponse>(
            crate::NodeTarget::Broker(SYNTH_ID),
            ApiKey::Metadata,
            1,
            metadata_request_build,
        ),
    )
    .await
    .expect("Client::send hung past the outer guard — acquisition timeout not wired")
    .unwrap_err();
    let elapsed = started.elapsed();

    match err {
        crate::Error::RequestTimeout(_) => {}
        other => panic!("expected RequestTimeout, got {other:?}"),
    }
    assert!(
        elapsed >= request_timeout && elapsed < request_timeout * 4,
        "acquisition timeout fired at unexpected time: {elapsed:?}"
    );
}

/// Concurrent-callers tear-down: while one `send` is stalled in the
/// wire phase, a second `send` races in on the same synthetic broker.
/// The first times out and calls `broker.shutdown()`, which drains the
/// second caller's in-flight slot with `ConnectionAborted`. That error
/// classifies as `RefreshMetadata`, validating the "cancel all
/// in-flight for that node" semantics.
#[tokio::test]
#[tracing_test::traced_test]
async fn request_timeout_tears_down_concurrent_callers() {
    let upstream = helpers::plaintext_broker().await;

    // After the handshake, delay every subsequent Metadata response.
    // The first two `send` calls both land on the same broker handle;
    // both get torn down when the first times out.
    let handshake_passed = Arc::new(AtomicUsize::new(0));
    let gate = handshake_passed.clone();
    let plan = FaultPlan {
        on_request: None,
        on_response: Some(Arc::new(move |_view: &ResponseView<'_>| {
            // Pass the first response (ApiVersions during handshake),
            // delay every response after that.
            let prev = gate.fetch_add(1, Ordering::SeqCst);
            if prev == 0 {
                None
            } else {
                Some(Fault::Delay(Duration::from_secs(30)))
            }
        })),
    };
    let proxy = helpers::proxy::start(&upstream.host, upstream.port, plan).await;

    let request_timeout = Duration::from_millis(400);
    let client = client_with_synthetic(&proxy.host, proxy.port, request_timeout).await;

    // Pre-dial so the second call sees a live `Slot::Resolved` and
    // skips the acquisition-phase branch.
    let _ = client.broker(SYNTH_ID).await.expect("pre-dial");

    let c1 = client.clone();
    let first = tokio::spawn(async move {
        c1.send::<_, kafka_protocol::messages::MetadataResponse>(
            crate::NodeTarget::Broker(SYNTH_ID),
            ApiKey::Metadata,
            1,
            metadata_request_build,
        )
        .await
    });

    // Give the first call time to enter the wire phase on the shared
    // broker connection before the second joins.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let c2 = client.clone();
    let second = tokio::spawn(async move {
        c2.send::<_, kafka_protocol::messages::MetadataResponse>(
            crate::NodeTarget::Broker(SYNTH_ID),
            ApiKey::Metadata,
            1,
            metadata_request_build,
        )
        .await
    });

    let first_err = first.await.unwrap().unwrap_err();
    let second_err = second.await.unwrap().unwrap_err();

    // First call: `RequestTimeout` from the wire-phase `timeout_at`.
    assert!(
        matches!(first_err, crate::Error::RequestTimeout(_)),
        "first call: expected RequestTimeout, got {first_err:?}"
    );

    // Second call: drained by `broker.shutdown()` via the in-flight
    // `drain_in_flight` path, which sends `ConnectionAborted`. That
    // classifies as `RefreshMetadata` per `classify_io_error`.
    match &second_err {
        crate::Error::Io(e) => assert_eq!(
            e.kind(),
            ErrorKind::ConnectionAborted,
            "second call io kind"
        ),
        crate::Error::RequestTimeout(_) => {
            // Acceptable race: the second call's own wire-phase budget
            // may have elapsed first if scheduling was slow.
        }
        other => panic!("second call: unexpected error {other:?}"),
    }
    assert_eq!(
        second_err.classify(),
        crate::error::RetryAction::RefreshMetadata,
        "second call must classify as RefreshMetadata, got {:?}",
        second_err.classify(),
    );
}

// ---------------------------------------------------------------------------
// Phase 2: retry loop
// ---------------------------------------------------------------------------

/// Bootstrap through the proxy using an address resolver that rewrites
/// every broker address to the proxy. Lets retry tests leave
/// `refresh_metadata()` in play: refreshes succeed, the resolver keeps
/// every broker id pointing at the proxy, and all retry attempts still
/// funnel through the fault injector.
async fn client_via_proxy_resolver(
    proxy: &ProxyHandle,
    request_timeout: Duration,
    api_timeout: Duration,
    retries: u32,
) -> crate::Client {
    let bootstrap = [crate::Config::new(&proxy.host, proxy.port)
        .with_request_timeout(request_timeout)
        .with_api_timeout(api_timeout)
        .with_retries(retries)
        .with_retry_backoff(Duration::from_millis(10))
        .with_retry_backoff_max(Duration::from_millis(50))];
    let proxy_host = proxy.host.clone();
    let proxy_port = proxy.port;
    crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |_id, _host, _port| Ok((proxy_host.clone(), proxy_port)),
    )
    .await
    .unwrap()
}

/// Wire-phase stall on the *first* Metadata request only; the retry
/// loop calls `refresh_metadata` (which the resolver keeps pointing
/// through the proxy), re-issues the request, and the second attempt
/// succeeds. Validates `RequestTimeout → RefreshMetadata → retry`
/// end-to-end.
#[tokio::test]
#[tracing_test::traced_test]
async fn retry_loop_recovers_after_request_timeout() {
    let upstream = helpers::plaintext_broker().await;

    // Delay only the first Metadata request; pass every request after
    // that. Dispatching from the request side lets us filter on
    // `api_key`, which keeps per-connection handshake ApiVersions out
    // of the counter.
    //
    // NB: bootstrap itself also fires a Metadata fetch through the
    // proxy, so by the time the test's `send` runs, several Metadata
    // requests have already passed. Using "first Metadata" here would
    // delay the bootstrap fetch; we want to delay the *test's* first
    // send, so gate on a flag the test arms after bootstrap.
    let armed = Arc::new(AtomicUsize::new(0));
    let hook = armed.clone();
    let plan = FaultPlan {
        on_request: Some(Arc::new(move |view: &RequestView<'_>| {
            if view.api_key == ApiKey::Metadata
                && hook
                    .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Some(Fault::Delay(Duration::from_secs(30)));
            }
            None
        })),
        on_response: None,
    };
    let proxy = helpers::proxy::start(&upstream.host, upstream.port, plan).await;

    let request_timeout = Duration::from_millis(300);
    let api_timeout = Duration::from_secs(10);
    let client = client_via_proxy_resolver(&proxy, request_timeout, api_timeout, 3).await;

    // Arm the fault: the *next* Metadata request (first test send)
    // stalls; any Metadata after that passes (the retry's second
    // attempt + any `refresh_metadata` in between).
    armed.store(1, Ordering::SeqCst);

    let started = Instant::now();
    let resp: kafka_protocol::messages::MetadataResponse = tokio::time::timeout(
        Duration::from_secs(15),
        client.send(
            crate::NodeTarget::AnyBroker,
            ApiKey::Metadata,
            1,
            metadata_request_build,
        ),
    )
    .await
    .expect("retry loop hung")
    .expect("retry loop must succeed");
    let elapsed = started.elapsed();

    assert!(!resp.brokers.is_empty());
    assert!(
        elapsed >= request_timeout && elapsed < api_timeout,
        "retry-loop recovery took {elapsed:?}"
    );
}

/// `api_timeout` caps the total wall-clock cost of the retry loop even
/// when the target is permanently stalled. Without the per-attempt
/// clamp, this would overshoot by up to one `request_timeout` on the
/// last attempt.
#[tokio::test]
#[tracing_test::traced_test]
async fn retry_loop_respects_api_timeout_cap() {
    let upstream = helpers::plaintext_broker().await;

    // Start the proxy with a transparent plan so bootstrap's Metadata
    // fetch succeeds; swap in the stall plan *after* the client is
    // built so every subsequent Metadata request burns its budget.
    let proxy = helpers::proxy::start(&upstream.host, upstream.port, FaultPlan::new()).await;

    let request_timeout = Duration::from_millis(200);
    // api_timeout deliberately not an integer multiple of
    // request_timeout so we can distinguish clamped behaviour from a
    // naive `retries * request_timeout` loop.
    let api_timeout = Duration::from_millis(700);
    // retries effectively unbounded — we want api_timeout to be the stop.
    let client = client_via_proxy_resolver(&proxy, request_timeout, api_timeout, 100).await;

    // Arm the stall: Metadata requests from here on delay past the
    // budget, handshake ApiVersions passes so reconnects still work.
    proxy.set_plan(FaultPlan {
        on_request: Some(Arc::new(|view: &RequestView<'_>| {
            if view.api_key == ApiKey::Metadata {
                Some(Fault::Delay(Duration::from_secs(60)))
            } else {
                None
            }
        })),
        on_response: None,
    });

    let started = Instant::now();
    let err = client
        .send::<_, kafka_protocol::messages::MetadataResponse>(
            crate::NodeTarget::AnyBroker,
            ApiKey::Metadata,
            1,
            metadata_request_build,
        )
        .await
        .unwrap_err();
    let elapsed = started.elapsed();

    assert!(
        matches!(err, crate::Error::RequestTimeout(_)),
        "expected RequestTimeout, got {err:?}"
    );
    // Loop must terminate near api_timeout, not at
    // `retries * request_timeout` (= 20 s for retries=100).
    assert!(
        elapsed < api_timeout + Duration::from_millis(500),
        "retry loop overshot api_timeout: {elapsed:?}"
    );
    assert!(
        elapsed >= api_timeout - Duration::from_millis(100),
        "retry loop returned before api_timeout: {elapsed:?}"
    );
}
