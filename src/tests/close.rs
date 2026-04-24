use std::time::Duration;

use super::helpers;

#[tokio::test]
#[tracing_test::traced_test]
async fn test_client_close_shuts_down_held_broker() {
    let broker = helpers::plaintext_broker().await;

    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    // Force the connection pool to populate and keep an external handle — this
    // is the case `close()` must cover: `broker()` clones `BrokerClient`, so
    // simply dropping `Client` wouldn't be enough to tear the connection down.
    let held = client.controller().await.unwrap();
    assert!(!held.is_shutdown());

    client.close();

    // The pool is cleared synchronously.
    assert_eq!(client.connection_slot_count(), 0);

    // Background `read_task` exits asynchronously after close().
    // "client requested shutdown" is emitted only from the
    // `close.notified()` select arm in read_task, so it's a uniquely-
    // keyed signal for the branch we want to exercise — and it cannot
    // be confused with the separate bootstrap-connection tear-down that
    // fires its own exit logs earlier when the bootstrap address
    // doesn't match a metadata entry (see `Client::connect` seeding).
    helpers::wait_for_log(
        || logs_contain("client requested shutdown"),
        Duration::from_secs(2),
        "read_task never saw the close.notified() signal",
    )
    .await;
    assert!(held.is_shutdown(), "broker did not shut down after close()");

    // Racing sends after close observe ConnectionAborted via the fast-path flag.
    let err = held.fetch_metadata().await.unwrap_err();
    match err {
        crate::Error::Io(e) if e.kind() == std::io::ErrorKind::ConnectionAborted => {}
        other => panic!("expected ConnectionAborted, got {other:?}"),
    }

    // Idempotency: second close must not panic.
    client.close();
}

/// Regression test for the spurious `"re-authentication failed"` ERROR log
/// on a clean, user-initiated shutdown.
///
/// With `Auth::None` the handshake stores `reauth_shutdown_tx` in
/// `_reauth_shutdown_tx` on the inner (no reauth task is spawned). When the
/// user drops the only external `BrokerClient` handle, `BrokerClientInner::drop`
/// notifies `close` and the parked sender is dropped as a struct field right
/// after. Both the `close.notified()` arm and the `reauth_shutdown` arm of
/// `read_task`'s biased `select!` become ready on the same poll, and the
/// biased ordering picks `reauth_shutdown` — producing an ERROR-level
/// `"re-authentication failed, shutting down connection"` log for what was
/// a clean close. Monitoring alerts watching for that message fire a false
/// positive on every normal shutdown.
#[tokio::test]
#[tracing_test::traced_test]
async fn test_clean_shutdown_does_not_log_reauth_error() {
    let broker = helpers::plaintext_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, crate::Auth::None, None)
        .await
        .unwrap();

    // Sanity: the connection is live.
    client.fetch_metadata().await.unwrap();

    // Drop the only external handle → `BrokerClientInner::drop` runs.
    drop(client);

    // Wait until `read_task` has exited so all tear-down log lines have landed.
    helpers::wait_for_log(
        || logs_contain("read task exiting"),
        Duration::from_secs(2),
        "read_task never exited after BrokerClient drop",
    )
    .await;

    logs_assert(|lines: &[&str]| {
        let saw_spurious_error = lines
            .iter()
            .any(|l| l.contains("re-authentication failed, shutting down connection"));
        if saw_spurious_error {
            return Err(
                "clean shutdown emitted spurious ERROR \"re-authentication failed, shutting down connection\" (Bug 4 regression)"
                    .into(),
            );
        }
        Ok(())
    });
}
