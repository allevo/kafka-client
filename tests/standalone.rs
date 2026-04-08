mod common;

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
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

#[tokio::test]
async fn test_standalone_fetch_metadata() {
    let kafka = Kafka::default().start().await.unwrap();

    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap();

    let config = kafka_client::Config::new(&host, port);
    let conn =
        kafka_client::Connection::connect(&config, kafka_client::Security::Plaintext, kafka_client::Auth::None)
            .await
            .unwrap();

    let client = kafka_client::BrokerClient::new(conn);
    let response = client.fetch_metadata().await.unwrap();

    // Should have at least one broker
    assert!(!response.brokers.is_empty());
    let broker = &response.brokers[0];
    assert!(broker.port > 0);
    assert!(!broker.host.is_empty());

    // Controller should be a valid node ID
    assert!(response.controller_id.0 >= 0);
}

#[tokio::test]
async fn test_standalone_cluster_client() {
    let kafka = Kafka::default().start().await.unwrap();

    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap();

    let bootstrap = [kafka_client::Config::new(&host, port)];
    let mut client = kafka_client::Client::connect(
        &bootstrap,
        kafka_client::Security::Plaintext,
        kafka_client::Auth::None,
    )
    .await
    .unwrap();

    // Controller should be discovered
    assert!(client.controller_id() >= 0);

    // Should be able to get the controller broker and use it
    let controller = client.controller().await.unwrap();
    let metadata = controller.fetch_metadata().await.unwrap();
    
    println!("{}", metadata.brokers.len());
    assert!(!metadata.brokers.is_empty());

    // refresh_metadata should work
    let refreshed = client.refresh_metadata().await.unwrap();
    assert!(!refreshed.brokers.is_empty());
    assert!(refreshed.controller_id.0 >= 0);
}

#[tokio::test]
async fn test_standalone_create_topic() {
    let kafka = Kafka::default().start().await.unwrap();

    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap();

    let bootstrap = [kafka_client::Config::new(&host, port)];
    let mut client = kafka_client::Client::connect(
        &bootstrap,
        kafka_client::Security::Plaintext,
        kafka_client::Auth::None,
    )
    .await
    .unwrap();

    let topic = CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("test-topic-1")))
        .with_num_partitions(3)
        .with_replication_factor(1);

    let response = client.create_topics(vec![topic], 5000).await.unwrap();
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name.as_str(), "test-topic-1");
    assert_eq!(response.topics[0].error_code, 0);

    // Verify the topic appears in metadata
    let metadata = client.refresh_metadata().await.unwrap();
    let found = metadata.topics.iter().find(|t| {
        t.name.as_ref().map(|n| n.as_str()) == Some("test-topic-1")
    });
    assert!(found.is_some(), "topic 'test-topic-1' not found in metadata");
    let Some(found) = found else { panic!() };
    assert_eq!(found.partitions.len(), 3);
    for partition in &found.partitions {
        assert!(*partition.leader_id >= 0);
    }
}
