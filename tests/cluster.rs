mod common;

use std::collections::HashMap;

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

#[tokio::test]
async fn test_cluster_client() {
    let prefix = "cc";
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

    // Build address resolver: node_id -> (host, mapped_port)
    let mut addr_map = HashMap::new();
    for (node_id, node) in [(1, &n1), (2, &n2), (3, &n3)] {
        let host = node.get_host().await.unwrap().to_string();
        let port = node.get_host_port_ipv4(common::KAFKA_PORT).await.unwrap();
        common::assert_tcp_reachable(&host, port).await;
        addr_map.insert(node_id, (host, port));
    }

    // Use first broker as bootstrap
    let (ref boot_host, boot_port) = addr_map[&1];
    let bootstrap = [kafka_client::Config::new(boot_host, boot_port)];

    let mut client = kafka_client::Client::connect_with_resolver(
        &bootstrap,
        kafka_client::Security::Plaintext,
        kafka_client::Auth::None,
        move |node_id, _host, _port| {
            addr_map[&node_id].clone()
        },
    )
    .await
    .unwrap();

    // Brokers may need time to fully register with each other in KRaft mode
    let mut metadata = client.refresh_metadata().await.unwrap();
    for _ in 0..10 {
        if metadata.brokers.len() == 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        metadata = client.refresh_metadata().await.unwrap();
    }
    assert_eq!(metadata.brokers.len(), 3);

    // Controller should be one of the 3 nodes
    let controller_id = client.controller_id();
    assert!(
        (1..=3).contains(&controller_id),
        "controller_id {controller_id} not in range 1..=3"
    );

    // Verify all 3 node IDs are present and distinct
    let mut node_ids: Vec<i32> = metadata.brokers.iter().map(|b| b.node_id.0).collect();
    node_ids.sort();
    assert_eq!(node_ids, vec![1, 2, 3]);

    // Should be able to connect to every broker by node_id
    // and each broker should eventually see all 3 peers
    for id in [client.controller_id(), 1, 2, 3] {
        let broker = client.broker(id).await.unwrap();
        let mut m = broker.fetch_metadata().await.unwrap();
        for _ in 0..10 {
            if m.brokers.len() == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            m = broker.fetch_metadata().await.unwrap();
        }
        assert_eq!(m.brokers.len(), 3, "broker {id} sees wrong broker count");
    }
}
