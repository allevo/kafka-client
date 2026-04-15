//! Tests for `Connection::connect`'s setup timeout (TIMEOUT.md §1.1,
//! narrowed to `Connection::connect` scope only — see plan).
//!
//! These tests pin the contract for the configurable wall-clock bound on
//! `Connection::connect`. They will FAIL until `Connection::connect` is
//! wrapped with `tokio::time::timeout` reading
//! `Config::connection_setup_timeout`. That production change is the
//! follow-up to this PR.
//!
//! Each test that exercises a stalled peer wraps its own outer
//! `tokio::time::timeout` so a missing implementation surfaces as a fast
//! failed assertion rather than hanging the test process.

use std::io::ErrorKind;
use std::time::{Duration, Instant};

use super::helpers;
use super::helpers::stall_listener::start_stall_listener;

/// Outer guard so a missing impl never hangs the suite. Generous enough
/// that a working impl with a 200ms inner bound will not race it.
const OUTER_GUARD: Duration = Duration::from_secs(2);

#[tokio::test]
async fn timeout_setup_expires_on_plain_tcp_stall() {
    // Peer accepts TCP but never sends the TLS ServerHello, so a working
    // implementation must trip the configured setup bound rather than hang
    // on the rustls handshake.
    let listener = start_stall_listener().await;
    let tls_config = helpers::tls::build_tls_config();

    let config = crate::Config::new(&listener.host, listener.port)
        .with_connection_setup_timeout(Duration::from_millis(200));

    let started = Instant::now();
    let result = tokio::time::timeout(
        OUTER_GUARD,
        crate::Connection::connect(&config, crate::Security::Ssl(tls_config)),
    )
    .await;
    let elapsed = started.elapsed();

    let inner =
        result.expect("Connection::connect hung past the outer guard — setup timeout is not wired");
    // `Connection` doesn't implement Debug, so we can't use `expect_err`.
    let err = match inner {
        Ok(_) => panic!("expected Connection::connect to time out, got Ok"),
        Err(e) => e,
    };
    match err {
        crate::Error::Io(io_err) => assert_eq!(
            io_err.kind(),
            ErrorKind::TimedOut,
            "unexpected io error kind: {:?}",
            io_err.kind()
        ),
        other => panic!("expected Io(TimedOut), got {other:?}"),
    }
    assert!(
        elapsed < Duration::from_secs(1),
        "setup timeout fired but took too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn timeout_setup_expires_when_peer_never_acks_tls() {
    // Plaintext sibling of the previous test. With `Security::Plaintext`
    // there is no application-level handshake after TCP, so a stall
    // listener accepts and `Connection::connect` is expected to RETURN
    // SUCCESSFULLY: the contract narrowed by the plan is "setup timeout =
    // TCP + TLS, nothing more". This test is the regression guard for that
    // design choice — if a future implementer extends the bound to also
    // wait for a first byte on plaintext, this test should be deleted, not
    // patched.
    let listener = start_stall_listener().await;

    let config = crate::Config::new(&listener.host, listener.port)
        .with_connection_setup_timeout(Duration::from_millis(200));

    let started = Instant::now();
    let result = tokio::time::timeout(
        OUTER_GUARD,
        crate::Connection::connect(&config, crate::Security::Plaintext),
    )
    .await;
    let elapsed = started.elapsed();

    let inner = result.expect("Connection::connect hung past the outer guard");
    inner.expect("plaintext connect against a stall listener should succeed");
    assert!(
        elapsed < Duration::from_millis(500),
        "plaintext connect took longer than expected: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn timeout_setup_happy_path() {
    // Sanity: a generous setup timeout against a real broker must not
    // trip. Guards against an implementation that spuriously fires on the
    // fast path.
    let broker = helpers::plaintext_broker().await;

    let config = crate::Config::new(&broker.host, broker.port)
        .with_connection_setup_timeout(Duration::from_secs(5));

    let started = Instant::now();
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .expect("connect against real broker with generous timeout");
    let elapsed = started.elapsed();

    // Make sure the connection is actually usable end-to-end and that the
    // setup bound didn't somehow short-circuit a half-built stream.
    let _client = crate::BrokerClient::new(conn, crate::Auth::None)
        .await
        .expect("broker client handshake against real broker");

    assert!(
        elapsed < Duration::from_secs(1),
        "connect to real broker was suspiciously slow: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn timeout_setup_unset_preserves_legacy_behavior() {
    // Pin the "default = None = off" semantics. With no setup timeout
    // configured, `Connection::connect` against a stall listener using TLS
    // must hang indefinitely (matching today's behavior) — the test
    // proves it by asserting the *outer* `tokio::time::timeout` is what
    // fires. If a future change ever defaults `connection_setup_timeout`
    // to some non-None value (e.g. 30s like librdkafka), this test will
    // fail loudly and force a deliberate decision.
    let listener = start_stall_listener().await;
    let tls_config = helpers::tls::build_tls_config();

    let config = crate::Config::new(&listener.host, listener.port);
    assert!(
        config.connection_setup_timeout.is_none(),
        "default setup timeout must be None"
    );

    let result = tokio::time::timeout(
        Duration::from_millis(500),
        crate::Connection::connect(&config, crate::Security::Ssl(tls_config)),
    )
    .await;

    assert!(
        result.is_err(),
        "with no setup timeout set, Connection::connect must hang; got: {:?}",
        result.ok().map(|inner| inner.is_ok())
    );
}
