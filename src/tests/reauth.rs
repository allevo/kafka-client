use std::time::Duration;

use super::helpers;

/// Verify that BrokerClient::connect succeeds against a broker that enforces re-auth.
#[tokio::test]
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

    assert!(successes >= 10, "expected at least 10 successes, got {successes}");
}
