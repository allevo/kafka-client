mod common;

use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;

#[tokio::test]
async fn test_cluster_plaintext() {
    let prefix = "pt";
    let network = &format!("{prefix}-network");
    let quorum_voters = common::quorum_voters_plaintext(prefix);

    let (n1, n2, n3) = tokio::try_join!(
        common::kraft_broker_plaintext(prefix, 1, &quorum_voters)
            .with_network(network)
            .start(),
        common::kraft_broker_plaintext(prefix, 2, &quorum_voters)
            .with_network(network)
            .start(),
        common::kraft_broker_plaintext(prefix, 3, &quorum_voters)
            .with_network(network)
            .start(),
    )
    .unwrap();

    for node in [&n1, &n2, &n3] {
        let host = node.get_host().await.unwrap();
        let port = node.get_host_port_ipv4(common::KAFKA_PORT).await.unwrap();
        common::assert_tcp_reachable(&host.to_string(), port).await;
    }
}
