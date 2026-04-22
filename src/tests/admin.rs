use std::time::Duration;

use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::StrBytes;

use crate::AdminOptions;

use super::helpers;

// Kafka ConfigResource.Type values (org.apache.kafka.common.config.ConfigResource.Type).
const RESOURCE_TYPE_TOPIC: i8 = 2;
// Incremental AlterConfigs op codes (org.apache.kafka.clients.admin.AlterConfigOp.OpType).
const ALTER_OP_SET: i8 = 0;

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

async fn create_topic(client: &crate::Client, name: &'static str, partitions: i32) {
    let topic = CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(name)))
        .with_num_partitions(partitions)
        .with_replication_factor(1);
    let response = client
        .admin()
        .create_topics(
            vec![topic],
            Some(Duration::from_secs(5)),
            AdminOptions::default(),
        )
        .await
        .unwrap();
    let code = response.topics[0].error_code;
    // Broker is shared; tolerate TOPIC_ALREADY_EXISTS (36) from earlier test runs.
    assert!(
        code == 0 || code == 36,
        "unexpected create_topics error code: {code}"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_delete_topics() {
    let client = connect().await;
    let name = "admin-delete-topics";
    create_topic(&client, name, 1).await;

    let response = client
        .admin()
        .delete_topics(
            vec![TopicName::from(StrBytes::from_static_str(name))],
            Some(Duration::from_secs(5)),
            AdminOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, 0);
    assert_eq!(
        response.responses[0].name.as_ref().map(|n| n.as_str()),
        Some(name)
    );

    let metadata = client.refresh_metadata().await.unwrap();
    let found = metadata
        .topics
        .iter()
        .any(|t| t.name.as_ref().map(|n| n.as_str()) == Some(name));
    assert!(!found, "topic '{name}' still present after delete_topics");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_create_partitions() {
    let client = connect().await;
    let name = "admin-create-partitions";
    create_topic(&client, name, 1).await;

    let topic = CreatePartitionsTopic::default()
        .with_name(TopicName::from(StrBytes::from_static_str(name)))
        .with_count(3)
        .with_assignments(None);
    let response = client
        .admin()
        .create_partitions(
            vec![topic],
            Some(Duration::from_secs(5)),
            false,
            AdminOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(response.results.len(), 1);
    let code = response.results[0].error_code;
    // Accept INVALID_PARTITIONS (37) if a previous run already expanded the topic.
    assert!(
        code == 0 || code == 37,
        "unexpected create_partitions error code: {code}"
    );

    let describe = client
        .admin()
        .describe_topics(
            vec![
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName::from(StrBytes::from_static_str(name)))),
            ],
            AdminOptions::default(),
        )
        .await
        .unwrap();
    let topic_md = describe
        .topics
        .iter()
        .find(|t| t.name.as_ref().map(|n| n.as_str()) == Some(name))
        .expect("topic not found in metadata");
    assert_eq!(topic_md.partitions.len(), 3);
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_list_topics() {
    let client = connect().await;
    let name = "admin-list-topics";
    create_topic(&client, name, 1).await;

    let response = client
        .admin()
        .list_topics(AdminOptions::default())
        .await
        .unwrap();
    let found = response
        .topics
        .iter()
        .any(|t| t.name.as_ref().map(|n| n.as_str()) == Some(name));
    assert!(found, "topic '{name}' missing from list_topics response");
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_describe_topics() {
    let client = connect().await;
    let name = "admin-describe-topics";
    create_topic(&client, name, 2).await;

    let response = client
        .admin()
        .describe_topics(
            vec![
                MetadataRequestTopic::default()
                    .with_name(Some(TopicName::from(StrBytes::from_static_str(name)))),
            ],
            AdminOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(response.topics.len(), 1);
    let topic = &response.topics[0];
    assert_eq!(topic.name.as_ref().map(|n| n.as_str()), Some(name));
    assert_eq!(topic.partitions.len(), 2);
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_describe_cluster() {
    let client = connect().await;

    let response = client
        .admin()
        .describe_cluster(false, AdminOptions::default())
        .await
        .unwrap();
    assert_eq!(response.error_code, 0);
    assert!(
        !response.brokers.is_empty(),
        "describe_cluster returned no brokers"
    );
    assert!(!response.cluster_id.is_empty());
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_describe_configs() {
    let client = connect().await;
    let name = "admin-describe-configs";
    create_topic(&client, name, 1).await;

    let resource = DescribeConfigsResource::default()
        .with_resource_type(RESOURCE_TYPE_TOPIC)
        .with_resource_name(StrBytes::from_static_str(name))
        .with_configuration_keys(None);
    let response = client
        .admin()
        .describe_configs(vec![resource], AdminOptions::default())
        .await
        .unwrap();
    assert_eq!(response.results.len(), 1);
    let result = &response.results[0];
    assert_eq!(result.error_code, 0);
    assert_eq!(result.resource_name.as_str(), name);
    assert!(
        result
            .configs
            .iter()
            .any(|c| c.name.as_str() == "retention.ms"),
        "retention.ms not present in describe_configs result"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_create_list_delete_list_create_list_flow() {
    let client = connect().await;
    let name = "admin-flow";
    let listed_has = |resp: &kafka_protocol::messages::MetadataResponse| {
        resp.topics
            .iter()
            .any(|t| t.name.as_ref().map(|n| n.as_str()) == Some(name))
    };

    create_topic(&client, name, 1).await;
    assert!(
        listed_has(
            &client
                .admin()
                .list_topics(AdminOptions::default())
                .await
                .unwrap()
        ),
        "topic '{name}' missing after first create"
    );

    let deleted = client
        .admin()
        .delete_topics(
            vec![TopicName::from(StrBytes::from_static_str(name))],
            Some(Duration::from_secs(5)),
            AdminOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.responses[0].error_code, 0);

    assert!(
        !listed_has(
            &client
                .admin()
                .list_topics(AdminOptions::default())
                .await
                .unwrap()
        ),
        "topic '{name}' still present after delete"
    );

    create_topic(&client, name, 1).await;
    assert!(
        listed_has(
            &client
                .admin()
                .list_topics(AdminOptions::default())
                .await
                .unwrap()
        ),
        "topic '{name}' missing after recreate"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_incremental_alter_configs() {
    let client = connect().await;
    let name = "admin-alter-configs";
    create_topic(&client, name, 1).await;

    let new_retention = "3600001";
    let config = AlterableConfig::default()
        .with_name(StrBytes::from_static_str("retention.ms"))
        .with_config_operation(ALTER_OP_SET)
        .with_value(Some(StrBytes::from_static_str(new_retention)));
    let resource = AlterConfigsResource::default()
        .with_resource_type(RESOURCE_TYPE_TOPIC)
        .with_resource_name(StrBytes::from_static_str(name))
        .with_configs(vec![config]);
    let response = client
        .admin()
        .incremental_alter_configs(vec![resource], false, AdminOptions::default())
        .await
        .unwrap();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, 0);

    let describe = client
        .admin()
        .describe_configs(
            vec![
                DescribeConfigsResource::default()
                    .with_resource_type(RESOURCE_TYPE_TOPIC)
                    .with_resource_name(StrBytes::from_static_str(name))
                    .with_configuration_keys(Some(vec![StrBytes::from_static_str("retention.ms")])),
            ],
            AdminOptions::default(),
        )
        .await
        .unwrap();
    let retention = describe.results[0]
        .configs
        .iter()
        .find(|c| c.name.as_str() == "retention.ms")
        .expect("retention.ms missing in describe_configs");
    assert_eq!(
        retention.value.as_ref().map(|v| v.as_str()),
        Some(new_retention)
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_options_timeout_override() {
    // A 1 ms api_timeout override trips the retry-loop deadline long before any
    // real RPC can complete; with_retries(0) skips the refresh/retry path so the
    // first failure returns immediately. Proves AdminOptions reaches Client::send
    // instead of being silently discarded.
    let client = connect().await;

    let started = std::time::Instant::now();
    let result = client
        .admin()
        .describe_cluster(
            false,
            AdminOptions::new()
                .with_timeout(Duration::from_millis(1))
                .with_retries(0),
        )
        .await;
    let elapsed = started.elapsed();

    assert!(
        matches!(result, Err(crate::Error::RequestTimeout(_))),
        "expected RequestTimeout, got: {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(1),
        "override ignored? call took {elapsed:?}"
    );
}
