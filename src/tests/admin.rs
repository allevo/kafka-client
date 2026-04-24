use std::time::Duration;

use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::StrBytes;

use crate::CallOptions;

use super::helpers;
use super::helpers::create_topic;

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
            CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, 0);
    assert_eq!(
        response.responses[0].name.as_ref().map(|n| n.as_str()),
        Some(name)
    );

    // DeleteTopics returns once the controller has committed the deletion;
    // the broker's MetadataCache replay lags behind. Wait before querying.
    helpers::wait_for_topic_gone_by_name(&client, name).await;

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
            CallOptions::default(),
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

    // CreatePartitions has the same KRaft MetadataCache-replay lag as
    // CreateTopics: the new partition count is committed by the controller
    // before the broker's cache catches up. Poll until it does.
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut backoff = Duration::from_millis(20);
    loop {
        let describe = client
            .admin()
            .describe_topics(
                vec![MetadataRequestTopic::default().with_name(Some(topic_name.clone()))],
                CallOptions::default(),
            )
            .await
            .unwrap();
        let topic_md = describe
            .topics
            .iter()
            .find(|t| t.name.as_ref() == Some(&topic_name))
            .expect("topic not found in metadata");
        if topic_md.partitions.len() == 3 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "partition count never caught up within 30s (got {})",
            topic_md.partitions.len()
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(200));
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_admin_list_topics() {
    let client = connect().await;
    let name = "admin-list-topics";
    create_topic(&client, name, 1).await;

    let response = client
        .admin()
        .list_topics(CallOptions::default())
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
            CallOptions::default(),
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
        .describe_cluster(false, CallOptions::default())
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
        .describe_configs(vec![resource], CallOptions::default())
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
                .list_topics(CallOptions::default())
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
            CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(deleted.responses[0].error_code, 0);
    helpers::wait_for_topic_gone_by_name(&client, name).await;

    assert!(
        !listed_has(
            &client
                .admin()
                .list_topics(CallOptions::default())
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
                .list_topics(CallOptions::default())
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
        .incremental_alter_configs(vec![resource], false, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].error_code, 0);

    // IncrementalAlterConfigs has the same KRaft controller-commit-vs-
    // broker-replay lag as CreateTopics: `describe_configs` can return the
    // old value for a short window after the alter acknowledges. Poll
    // until the broker's cache catches up.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut backoff = Duration::from_millis(20);
    loop {
        let describe = client
            .admin()
            .describe_configs(
                vec![
                    DescribeConfigsResource::default()
                        .with_resource_type(RESOURCE_TYPE_TOPIC)
                        .with_resource_name(StrBytes::from_static_str(name))
                        .with_configuration_keys(Some(vec![StrBytes::from_static_str(
                            "retention.ms",
                        )])),
                ],
                CallOptions::default(),
            )
            .await
            .unwrap();
        let retention = describe.results[0]
            .configs
            .iter()
            .find(|c| c.name.as_str() == "retention.ms")
            .expect("retention.ms missing in describe_configs");
        if retention.value.as_ref().map(|v| v.as_str()) == Some(new_retention) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "retention.ms never reflected the new value within 30s (got {:?})",
            retention.value.as_ref().map(|v| v.as_str())
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(200));
    }
}

#[tokio::test]
#[tracing_test::traced_test]
async fn test_call_options_timeout_override() {
    // A zero api_timeout override trips the retry-loop deadline check on the
    // very first iteration, before any RPC can fire; with_retries(0) skips the
    // refresh/retry path so the first failure returns immediately. Proves
    // CallOptions reaches Client::send instead of being silently discarded —
    // the default api_timeout is never zero, so a successful response here
    // would mean the override was dropped. A non-zero value (e.g. 1 ms) is
    // racy: a warm local broker can answer DescribeCluster within that window
    // on a fast runner.
    let client = connect().await;

    let started = std::time::Instant::now();
    let result = client
        .admin()
        .describe_cluster(
            false,
            CallOptions::new()
                .with_timeout(Duration::ZERO)
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
