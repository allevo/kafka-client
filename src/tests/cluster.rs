use kafka_protocol::messages::BrokerId;

use super::helpers;

#[tokio::test]
async fn test_cluster_plaintext() {
    let cluster = helpers::plaintext_cluster().await;

    for (_node_id, (host, port)) in &cluster.addr_map {
        helpers::assert_tcp_reachable(host, *port).await;
    }
}

#[tokio::test]
async fn test_cluster_client() {
    let cluster = helpers::plaintext_cluster().await;

    let addr_map = cluster.addr_map.clone();
    let (ref boot_host, boot_port) = addr_map[&BrokerId(1)];
    let bootstrap = [crate::Config::new(boot_host, boot_port)];

    let mut client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |node_id, _host, _port| addr_map[&node_id].clone(),
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

    let controller_id = client.controller_id();
    assert!(
        (1..=3).contains(&controller_id.0),
        "controller_id {} not in range 1..=3",
        controller_id.0
    );

    let mut node_ids: Vec<BrokerId> = metadata.brokers.iter().map(|b| b.node_id).collect();
    node_ids.sort_by_key(|id| id.0);
    assert_eq!(node_ids, vec![BrokerId(1), BrokerId(2), BrokerId(3)]);

    // Each broker should eventually see all 3 peers
    for id in [client.controller_id(), BrokerId(1), BrokerId(2), BrokerId(3)] {
        let broker = client.broker(id).await.unwrap();
        let mut m = broker.fetch_metadata().await.unwrap();
        for _ in 0..10 {
            if m.brokers.len() == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            m = broker.fetch_metadata().await.unwrap();
        }
        assert_eq!(m.brokers.len(), 3, "broker {} sees wrong broker count", id.0);
    }
}
