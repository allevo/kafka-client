use std::time::Duration;

use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::protocol::StrBytes;

use crate::CallOptions;
use crate::admin::{CreateTopicsOptions, DeleteTopicsOptions};

/// Create a topic and wait until every broker reports it as visible.
///
/// `AdminClient::create_topics` already waits when
/// `wait_for_propagation = true` (the default), which is why this helper
/// is now a thin shim. Tolerates `TOPIC_ALREADY_EXISTS` (36) because the
/// shared broker carries state across test runs — the wait is still
/// performed in that case.
pub async fn create_topic(client: &crate::Client, name: &'static str, partitions: i32) {
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    let request = CreatableTopic::default()
        .with_name(topic_name.clone())
        .with_num_partitions(partitions)
        .with_replication_factor(1);
    let response = client
        .admin()
        .create_topics(
            vec![request],
            CreateTopicsOptions {
                broker_operation_timeout: Some(Duration::from_secs(30)),
                wait_for_propagation: true,
            },
            CallOptions::default(),
        )
        .await
        .unwrap();
    let code = response.topics[0].error_code;
    assert!(
        code == 0 || code == 36,
        "unexpected create_topics error code: {code}"
    );
}

/// Delete a topic and wait until every broker stops reporting it.
///
/// Mirror of [`create_topic`] — relies on `delete_topics` doing the
/// per-broker propagation wait by default. Tolerates
/// `UNKNOWN_TOPIC_OR_PARTITION` (3).
pub async fn delete_topic(client: &crate::Client, name: &'static str) {
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    let response = client
        .admin()
        .delete_topics(
            vec![topic_name.clone()],
            DeleteTopicsOptions {
                broker_operation_timeout: Some(Duration::from_secs(30)),
                wait_for_propagation: true,
            },
            CallOptions::default(),
        )
        .await
        .unwrap();
    let code = response.responses[0].error_code;
    assert!(
        code == 0 || code == 3,
        "unexpected delete_topics error code: {code}"
    );
}

/// Block until every broker reports `name` as visible. Panics on timeout.
///
/// Exposed for tests that create topics inline (with
/// `wait_for_propagation = false`, to assert on the raw
/// `CreateTopicsResponse`) and only need the wait step.
pub async fn wait_for_topic_visible_by_name(client: &crate::Client, name: &'static str) {
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    client
        .admin()
        .wait_for_topics_visible(&[topic_name], Duration::from_secs(30))
        .await
        .unwrap();
}

/// Block until every broker reports `name` as gone. Panics on timeout.
pub async fn wait_for_topic_gone_by_name(client: &crate::Client, name: &'static str) {
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    client
        .admin()
        .wait_for_topics_gone(&[topic_name], Duration::from_secs(30))
        .await
        .unwrap();
}
