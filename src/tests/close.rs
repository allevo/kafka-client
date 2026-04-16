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

    // Background `read_task` exit is async — poll briefly for the shutdown flag.
    let mut attempts = 0;
    while !held.is_shutdown() && attempts < 50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        attempts += 1;
    }
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
