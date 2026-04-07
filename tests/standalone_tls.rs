mod common;

use std::io::BufReader;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use testcontainers::runners::AsyncRunner;

fn fixtures_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("secrets")
}

fn load_certs(path: &std::path::Path) -> Vec<CertificateDer<'static>> {
    let file = std::fs::File::open(path).unwrap();
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

fn load_private_key(path: &std::path::Path) -> PrivateKeyDer<'static> {
    let file = std::fs::File::open(path).unwrap();
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader).unwrap().unwrap()
}

fn build_tls_config() -> Arc<rustls::ClientConfig> {
    let fixtures = fixtures_path();

    let ca_certs = load_certs(&fixtures.join("ca.pem"));
    let client_certs = load_certs(&fixtures.join("client.pem"));
    let client_key = load_private_key(&fixtures.join("client.key"));

    let mut root_store = rustls::RootCertStore::empty();
    for cert in ca_certs {
        root_store.add(cert).unwrap();
    }

    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(root_store)
    .with_client_auth_cert(client_certs, client_key)
    .unwrap();

    Arc::new(config)
}

#[tokio::test]
async fn test_standalone_tls_api_versions() {
    let kafka = common::standalone_tls_broker().start().await.unwrap();

    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka.get_host_port_ipv4(common::SSL_PORT).await.unwrap();

    let tls_config = build_tls_config();
    let config = kafka_client::Config::new(&host, port);
    let conn = kafka_client::Connection::connect(&config, kafka_client::Security::Ssl(tls_config))
        .await
        .unwrap();

    let versions = conn.api_versions();
    assert!(!versions.is_empty());
    assert!(versions.iter().any(|v| v.api_key == 18));
}
