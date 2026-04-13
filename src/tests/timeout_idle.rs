//! Tests for `connections.max.idle` (IDLE_TIMEOUT.md).

use std::time::Duration;

use super::helpers;

#[tokio::test]
async fn idle_happy_path_under_load() {
    // Tight loop of metadata requests must keep the connection alive:
    // every response bumps `last_activity`, so the idle arm never fires.
    let broker = helpers::plaintext_broker().await;

    let config = crate::Config::new(&broker.host, broker.port)
        .with_connections_max_idle(Duration::from_secs(1));
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, crate::Auth::None)
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        client
            .fetch_metadata()
            .await
            .expect("metadata under load should succeed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn idle_close_fires_when_connection_is_idle() {
    let broker = helpers::plaintext_broker().await;

    let config = crate::Config::new(&broker.host, broker.port)
        .with_connections_max_idle(Duration::from_millis(500));
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, crate::Auth::None)
        .await
        .unwrap();

    // Prime the connection so `last_activity` is fresh, then sit idle past
    // the configured bound.
    client.fetch_metadata().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1200)).await;

    // A follow-up request must now fail fast: the idle arm closed the
    // socket, the shared shutdown flag is set, and `send` rejects new work.
    let err = client
        .fetch_metadata()
        .await
        .expect_err("expected ConnectionAborted after idle close");
    match err {
        crate::Error::Io(io_err) => assert_eq!(
            io_err.kind(),
            std::io::ErrorKind::ConnectionAborted,
            "unexpected io kind: {:?}",
            io_err.kind()
        ),
        other => panic!("expected Io(ConnectionAborted), got {other:?}"),
    }
}

#[tokio::test]
async fn idle_disabled_never_closes() {
    let broker = helpers::plaintext_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    assert!(config.connections_max_idle.is_none());

    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, crate::Auth::None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_secs(3)).await;
    client
        .fetch_metadata()
        .await
        .expect("metadata must succeed when idle-close is disabled");
}
