use super::helpers;

#[tokio::test]
async fn test_standalone_tls_api_versions() {
    let broker = helpers::tls_broker().await;

    let tls_config = helpers::tls::build_tls_config();
    let config = crate::Config::new(&broker.host, broker.port);
    let conn = crate::Connection::connect(
        &config,
        crate::Security::Ssl(tls_config),
        crate::Auth::None,
    )
    .await
    .unwrap();

    let versions = conn.api_versions();
    assert!(!versions.is_empty());
    assert!(versions.iter().any(|v| v.api_key == 18));
}
