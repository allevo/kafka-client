mod common;

use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;

#[tokio::test]
async fn test_cluster_tls() {
    let network = "kafka-tls-test";
    let quorum_voters = "1@kafka-1:29092,2@kafka-2:29092,3@kafka-3:29092";

    let (n1, n2, n3) = tokio::try_join!(
        common::kraft_broker_tls(1, quorum_voters)
            .with_network(network)
            .start(),
        common::kraft_broker_tls(2, quorum_voters)
            .with_network(network)
            .start(),
        common::kraft_broker_tls(3, quorum_voters)
            .with_network(network)
            .start(),
    )
    .unwrap();

    for node in [&n1, &n2, &n3] {
        let host = node.get_host().await.unwrap();
        let port = node.get_host_port_ipv4(common::SSL_PORT).await.unwrap();
        common::assert_tcp_reachable(&host.to_string(), port).await;
    }
}
