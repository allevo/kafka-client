mod common;

use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};

#[tokio::test]
async fn test_standalone_plaintext() {
    let kafka = Kafka::default().start().await.unwrap();

    let host = kafka.get_host().await.unwrap();
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap();

    common::assert_tcp_reachable(&host.to_string(), port).await;
}

#[tokio::test]
async fn test_standalone_api_versions() {
    let kafka = Kafka::default().start().await.unwrap();

    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap();

    let config = kafka_client::Config::new(&host, port);
    let conn =
        kafka_client::Connection::connect(&config, kafka_client::Security::Plaintext, kafka_client::Auth::None)
            .await
            .unwrap();

    let versions = conn.api_versions();
    assert!(!versions.is_empty());
    assert!(versions.iter().any(|v| v.api_key == 18));
}
