use std::time::{Duration, Instant};

use kafka_protocol::messages::TopicName;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::StrBytes;

use crate::CallOptions;

/// Create a topic and wait until the broker's MetadataCache reports it.
///
/// In KRaft mode, `CreateTopics` returns as soon as the controller has
/// committed the topic-creation records; each broker's MetadataCache is
/// updated asynchronously as it replays the metadata log. A
/// `MetadataRequest` issued immediately after `CreateTopics` can therefore
/// race the replay and omit the new topic. Tests that assert on metadata
/// must wait for the replay to catch up, so this helper polls
/// `describe_topics` until the topic surfaces without an error.
///
/// Tolerates `TOPIC_ALREADY_EXISTS` (36) because the shared broker carries
/// state across test runs.
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
            Some(Duration::from_secs(5)),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let code = response.topics[0].error_code;
    assert!(
        code == 0 || code == 36,
        "unexpected create_topics error code: {code}"
    );

    wait_for_topic_visible(client, &topic_name).await;
}

/// Delete a topic and wait until the broker's MetadataCache stops reporting it.
///
/// Mirror of [`create_topic`]: `DeleteTopics` has the same
/// controller-commit-vs-broker-replay lag in KRaft mode, so tests that
/// assert on metadata after a delete must wait for the topic to disappear.
///
/// Tolerates `UNKNOWN_TOPIC_OR_PARTITION` (3) if the topic was never
/// created or was deleted by a previous run.
pub async fn delete_topic(client: &crate::Client, name: &'static str) {
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    let response = client
        .admin()
        .delete_topics(
            vec![topic_name.clone()],
            Some(Duration::from_secs(5)),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let code = response.responses[0].error_code;
    assert!(
        code == 0 || code == 3,
        "unexpected delete_topics error code: {code}"
    );

    wait_for_topic_gone(client, &topic_name).await;
}

/// Poll `describe_topics` until the broker reports `name` as visible
/// (`error_code == 0`). Panics on timeout.
///
/// Exposed for tests that create topics inline (to assert on the raw
/// `CreateTopicsResponse`) and only need the wait step.
pub async fn wait_for_topic_visible_by_name(client: &crate::Client, name: &'static str) {
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    wait_for_topic_visible(client, &topic_name).await;
}

/// Poll `describe_topics` until the broker reports `name` as gone
/// (`UNKNOWN_TOPIC_OR_PARTITION` or absent). Panics on timeout.
///
/// Exposed for tests that delete topics inline (to assert on the raw
/// `DeleteTopicsResponse`) and only need the wait step.
pub async fn wait_for_topic_gone_by_name(client: &crate::Client, name: &'static str) {
    let topic_name = TopicName::from(StrBytes::from_static_str(name));
    wait_for_topic_gone(client, &topic_name).await;
}

async fn wait_for_topic_visible(client: &crate::Client, topic_name: &TopicName) {
    const TIMEOUT: Duration = Duration::from_secs(30);
    let deadline = Instant::now() + TIMEOUT;
    let mut backoff = Duration::from_millis(20);

    loop {
        let resp = client
            .admin()
            .describe_topics(
                vec![MetadataRequestTopic::default().with_name(Some(topic_name.clone()))],
                CallOptions::default(),
            )
            .await
            .unwrap();

        // Any non-zero code (typically UNKNOWN_TOPIC_OR_PARTITION = 3)
        // means the broker's MetadataCache hasn't replayed the creation
        // record yet.
        let visible = resp
            .topics
            .iter()
            .any(|t| t.name.as_ref() == Some(topic_name) && t.error_code == 0);
        if visible {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "topic '{}' never became visible in metadata within {TIMEOUT:?}",
            topic_name.as_str()
        );

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(200));
    }
}

async fn wait_for_topic_gone(client: &crate::Client, topic_name: &TopicName) {
    const TIMEOUT: Duration = Duration::from_secs(30);
    let deadline = Instant::now() + TIMEOUT;
    let mut backoff = Duration::from_millis(20);

    loop {
        let resp = client
            .admin()
            .describe_topics(
                vec![MetadataRequestTopic::default().with_name(Some(topic_name.clone()))],
                CallOptions::default(),
            )
            .await
            .unwrap();

        // `describe_topics` always returns an entry for requested names.
        // UNKNOWN_TOPIC_OR_PARTITION (3) is the signal that the replay
        // has caught up with the deletion.
        let gone = resp
            .topics
            .iter()
            .find(|t| t.name.as_ref() == Some(topic_name))
            .is_none_or(|t| t.error_code == 3);
        if gone {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "topic '{}' never disappeared from metadata within {TIMEOUT:?}",
            topic_name.as_str()
        );

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(200));
    }
}
