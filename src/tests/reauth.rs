use std::time::Duration;

use super::helpers;

/// Verify that BrokerClient::connect succeeds against a broker that enforces re-auth.
#[tokio::test]
#[tracing_test::traced_test]
async fn test_session_lifetime_ms_parsed() {
    let broker = helpers::sasl_reauth_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let auth = crate::Auth::Plain {
        username: "admin".into(),
        password: crate::SecretString::new("admin-secret".into()),
    };
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, auth).await.unwrap();

    let versions = client.api_versions();
    assert!(!versions.is_empty());
}

/// Verify that the connection survives past the session lifetime by periodically sending
/// metadata requests over a period longer than `connections.max.reauth.ms` (5 s).
/// Without re-auth the broker would kill the connection.
#[tokio::test]
#[tracing_test::traced_test]
async fn test_connection_survives_reauth() {
    let broker = helpers::sasl_reauth_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let auth = crate::Auth::Plain {
        username: "admin".into(),
        password: crate::SecretString::new("admin-secret".into()),
    };
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, auth).await.unwrap();

    // Send requests over 12 seconds — well past the 5-second session lifetime.
    // If re-auth fails the broker kills the connection and fetch_metadata returns an error.
    let start = tokio::time::Instant::now();
    let mut successes = 0u32;
    while start.elapsed() < Duration::from_secs(12) {
        let resp = client.fetch_metadata().await;
        assert!(
            resp.is_ok(),
            "fetch_metadata failed after {:?}: {}",
            start.elapsed(),
            resp.unwrap_err()
        );
        successes += 1;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    assert!(
        successes >= 10,
        "expected at least 10 successes, got {successes}"
    );

    // Verify the background reauth task actually fired and succeeded.
    // With a 5-second session lifetime over 12 seconds, at least 2 re-auths should occur.
    logs_assert(|lines: &[&str]| {
        let reauth_count = lines
            .iter()
            .filter(|line| line.contains("re-authentication successful"))
            .count();
        if reauth_count >= 2 {
            Ok(())
        } else {
            Err(format!(
                "expected at least 2 re-authentications, but found {reauth_count}"
            ))
        }
    });
}

/// Regression test for the bug where `reauth_task` held a full `BrokerClient`
/// clone (and therefore a `mpsc::Sender<RequestMsg>`), preventing the connection
/// from being torn down when the user dropped their last external handle.
///
/// Before the fix, dropping `client` would leave three tokio tasks alive
/// indefinitely: `reauth_task` would keep waking on its sleep timer, issue
/// SASL handshake requests through its retained `request_tx`, and reset
/// `read_task`'s idle counter — so the FD and tasks leaked forever on any
/// SASL/reauth broker.
///
/// After the fix, `BrokerClientInner::drop` fires when the last external
/// handle is released, signalling `close` and `reauth_close` so all three
/// background tasks exit cleanly.
#[tokio::test]
#[tracing_test::traced_test]
async fn test_drop_tears_down_reauth_task_and_connection() {
    let broker = helpers::sasl_reauth_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let auth = crate::Auth::Plain {
        username: "admin".into(),
        password: crate::SecretString::new("admin-secret".into()),
    };
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, auth).await.unwrap();

    // Sanity-check that the connection is live and the reauth task was spawned
    // (the broker advertises a 5 s session lifetime, so reauth must be active).
    client.fetch_metadata().await.unwrap();

    // Drop the only external handle. With the bug, this would do nothing;
    // with the fix, `BrokerClientInner::drop` notifies `close`/`reauth_close`
    // and the cascade tears all three background tasks down.
    drop(client);

    // The cascade is `Notify`-permit driven (no I/O, no timers), so cleanup
    // completes on the next runtime poll. 1 s is a generous safety margin
    // and is also well under the next reauth cycle (~4.25 s) — so if the
    // bug is back, the regression test still finishes long before the leaked
    // reauth_task would fire its first re-authentication.
    tokio::time::sleep(Duration::from_secs(1)).await;

    logs_assert(|lines: &[&str]| {
        let saw_reauth_spawn = lines.iter().any(|l| l.contains("spawning re-auth task"));
        // Either exit path is acceptable: the `close` permit set by Drop
        // (the common case), or `Weak::upgrade` returning `None` if the
        // task happened to wake from sleep first.
        let reauth_exited = lines.iter().any(|l| {
            l.contains("reauth task observed shutdown, exiting")
                || l.contains("reauth task: client dropped, exiting")
        });
        let read_exited = lines.iter().any(|l| l.contains("read task exiting"));
        let write_exited = lines.iter().any(|l| l.contains("write task exiting"));
        // No re-auth should have completed during the 1 s window — if one
        // did, the dropped client somehow stayed alive (regression).
        let reauth_fired = lines.iter().any(|l| l.contains("re-auth timer fired"));

        if !saw_reauth_spawn {
            return Err("re-auth task was never spawned — broker did not advertise a session lifetime, test setup is wrong".into());
        }
        if reauth_fired {
            return Err(
                "reauth_task ran a re-auth cycle after BrokerClient was dropped (Bug 4 regression)"
                    .into(),
            );
        }
        if !reauth_exited {
            return Err(
                "reauth_task did not exit after BrokerClient drop (Bug 4 regression)".into(),
            );
        }
        if !read_exited {
            return Err("read_task did not exit after BrokerClient drop".into());
        }
        if !write_exited {
            return Err("write_task did not exit after BrokerClient drop".into());
        }
        Ok(())
    });
}
