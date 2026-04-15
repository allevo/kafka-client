use super::helpers;

#[tokio::test]
async fn test_standalone_tls_api_versions() {
    let _ = tracing_subscriber::fmt::try_init();
    let broker = helpers::tls_broker().await;

    let tls_config = helpers::tls::build_tls_config();
    let config = crate::Config::new(&broker.host, broker.port);
    let conn = crate::Connection::connect(&config, crate::Security::Ssl(tls_config))
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, crate::Auth::None)
        .await
        .unwrap();

    let versions = client.api_versions();
    assert!(!versions.is_empty());
    assert!(versions.iter().any(|v| v.api_key == 18));
}
