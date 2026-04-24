use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::protocol::StrBytes;

use super::helpers;

#[tokio::test]
#[tracing_test::traced_test]
async fn test_standalone_plaintext() {
    let broker = helpers::plaintext_broker().await;
    helpers::assert_tcp_reachable(&broker.host, broker.port).await;
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_standalone_api_versions() {
    let broker = helpers::plaintext_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    let client = crate::BrokerClient::new(conn, crate::Auth::None, None)
        .await
        .unwrap();

    let versions = client.api_versions();
    assert!(!versions.is_empty());
    assert!(versions.iter().any(|v| v.api_key == 18));
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_standalone_fetch_metadata() {
    let broker = helpers::plaintext_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();

    let client = crate::BrokerClient::new(conn, crate::Auth::None, None)
        .await
        .unwrap();
    let response = client.fetch_metadata().await.unwrap();

    assert!(!response.brokers.is_empty());
    let broker_info = &response.brokers[0];
    assert!(broker_info.port > 0);
    assert!(!broker_info.host.is_empty());

    assert!(*response.controller_id >= 0);
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_standalone_cluster_client() {
    let broker = helpers::plaintext_broker().await;

    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    assert!(*client.controller_id() >= 0);

    let controller = client.controller().await.unwrap();
    let metadata = controller.fetch_metadata().await.unwrap();
    assert!(!metadata.brokers.is_empty());

    let refreshed = client.refresh_metadata().await.unwrap();
    assert!(!refreshed.brokers.is_empty());
    assert!(*refreshed.controller_id >= 0);
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_standalone_create_topic() {
    let broker = helpers::plaintext_broker().await;

    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    let topic = CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str("test-topic-1")))
        .with_num_partitions(3)
        .with_replication_factor(1);

    let response = client
        .admin()
        .create_topics(
            vec![topic],
            Some(std::time::Duration::from_secs(5)),
            crate::CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name.as_str(), "test-topic-1");
    // Accept both 0 (success) and 36 (TOPIC_ALREADY_EXISTS) since the broker is shared
    assert!(
        response.topics[0].error_code == 0 || response.topics[0].error_code == 36,
        "unexpected error code: {}",
        response.topics[0].error_code
    );

    // CreateTopics returns once the controller has committed; the broker's
    // MetadataCache replay lags. Wait before asking any broker for metadata.
    helpers::wait_for_topic_visible_by_name(&client, "test-topic-1").await;

    let metadata = client.refresh_metadata().await.unwrap();
    let found = metadata
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some("test-topic-1"));
    assert!(
        found.is_some(),
        "topic 'test-topic-1' not found in metadata"
    );
    let Some(found) = found else { panic!() };
    assert_eq!(found.partitions.len(), 3);
    for partition in &found.partitions {
        assert!(*partition.leader_id >= 0);
    }
}
