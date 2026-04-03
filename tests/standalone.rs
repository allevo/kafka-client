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
