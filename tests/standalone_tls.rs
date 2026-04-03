mod common;

use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_standalone_tls() {
    let kafka = common::standalone_tls_broker().start().await.unwrap();

    let host = kafka.get_host().await.unwrap();
    let port = kafka.get_host_port_ipv4(common::SSL_PORT).await.unwrap();

    common::assert_tcp_reachable(&host.to_string(), port).await;
}
