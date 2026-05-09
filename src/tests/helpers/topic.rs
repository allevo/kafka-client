use std::time::{Duration, Instant};

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::{ApiKey, BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::CallOptions;
use crate::client::NodeTarget;

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
    // In a multi-broker cluster `describe_topics(AnyBroker)` only confirms
    // the replay reached *one* broker — but the producer's leader for a
    // partition may be a sibling whose MetadataCache is still behind,
    // and that broker would reject the produce with
    // UNKNOWN_TOPIC_OR_PARTITION. Poll every known broker individually.
    //
    // Topic-level `error_code == 0` is necessary but not sufficient:
    // KRaft replays the topic-creation record into MetadataCache and
    // ReplicaManager separately, so a broker can advertise the topic
    // before any partition has an elected leader (`leader_id == -1`,
    // or per-partition `LEADER_NOT_AVAILABLE` / `NOT_LEADER_OR_FOLLOWER`).
    // Sending a ProduceRequest in that window earns `NotLeaderOrFollower`,
    // and the producer doesn't yet retry per-partition (slice-2 work).
    // Wait until every partition is assigned to a real leader, on every
    // broker, before returning.
    poll_topic_state(client, topic_name, |topic| {
        topic.error_code == 0
            && !topic.partitions.is_empty()
            && topic
                .partitions
                .iter()
                .all(|p| p.error_code == 0 && *p.leader_id >= 0)
    })
    .await;
}

async fn wait_for_topic_gone(client: &crate::Client, topic_name: &TopicName) {
    // Symmetric to `wait_for_topic_visible`: deletions also propagate
    // per-broker, so the topic must be gone (or UNKNOWN_TOPIC_OR_PARTITION)
    // on every broker before the replay is fully caught up.
    poll_topic_state(client, topic_name, |topic| topic.error_code == 3).await;
}

/// Refresh metadata to learn every broker id, then poll each broker's
/// `MetadataRequest` view of `topic_name` until `accept(topic)` is true
/// on all of them. Panics on timeout.
///
/// A topic missing from the response is synthesised as a
/// `MetadataResponseTopic` with `error_code = UNKNOWN_TOPIC_OR_PARTITION`
/// (3) and no partitions, so callers can express the gone-check in the
/// same form as the visible-check.
async fn poll_topic_state(
    client: &crate::Client,
    topic_name: &TopicName,
    accept: impl Fn(&MetadataResponseTopic) -> bool,
) {
    const TIMEOUT: Duration = Duration::from_secs(30);
    let deadline = Instant::now() + TIMEOUT;
    let mut backoff = Duration::from_millis(20);

    let metadata = client.refresh_metadata().await.unwrap();
    let broker_ids: Vec<BrokerId> = metadata.brokers.iter().map(|b| b.node_id).collect();
    assert!(
        !broker_ids.is_empty(),
        "no brokers known to client — cannot wait for topic state"
    );

    loop {
        let mut all_ok = true;
        for &broker_id in &broker_ids {
            let request = MetadataRequest::default().with_topics(Some(vec![
                MetadataRequestTopic::default().with_name(Some(topic_name.clone())),
            ]));
            let resp: MetadataResponse = client
                .send(
                    NodeTarget::Broker(broker_id),
                    ApiKey::Metadata,
                    9,
                    request,
                    &CallOptions::default(),
                )
                .await
                .unwrap();
            // `MetadataRequest` always echoes back an entry per requested
            // topic; treat a missing entry as `UNKNOWN_TOPIC_OR_PARTITION`
            // so the gone-check matches the deletion case symmetrically.
            let synthetic_missing;
            let topic = match resp
                .topics
                .iter()
                .find(|t| t.name.as_ref() == Some(topic_name))
            {
                Some(t) => t,
                None => {
                    synthetic_missing =
                        MetadataResponseTopic::default().with_error_code(3);
                    &synthetic_missing
                }
            };
            if !accept(topic) {
                all_ok = false;
                break;
            }
        }
        if all_ok {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "topic '{}' never reached expected state on all brokers within {TIMEOUT:?}",
            topic_name.as_str()
        );

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(200));
    }
}
