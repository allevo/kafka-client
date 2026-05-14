//! Topic create/delete for the harness, via this library's own
//! `Client` + `AdminClient`.
//!
//! Mirrors `src/tests/helpers/topic.rs`, but takes owned `String` names
//! (the test helper is `&'static str`-only): bench topic names are built
//! at runtime, so `TopicName` comes from `StrBytes::from_string` rather
//! than `from_static_str`.

use std::time::Duration;

use kafka_client::admin::{CreateTopicsOptions, DeleteTopicsOptions};
use kafka_client::{CallOptions, Client, StrBytes, TopicName};
use kafka_protocol::messages::create_topics_request::CreatableTopic;

/// Wall-clock budget for the controller RPC plus its propagation wait.
const TOPIC_OP_TIMEOUT: Duration = Duration::from_secs(30);

// Kafka per-topic error codes treated as success: the topic ends up in the
// state we wanted regardless. 36 = TOPIC_ALREADY_EXISTS, 3 =
// UNKNOWN_TOPIC_OR_PARTITION.
const ERR_TOPIC_ALREADY_EXISTS: i16 = 36;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Create `name` with `partitions` partitions (replication factor 1) and
/// wait until every broker reports it visible with elected leaders.
pub async fn create_topic(client: &Client, name: &str, partitions: i32) -> anyhow::Result<()> {
    let topic_name = TopicName::from(StrBytes::from_string(name.to_owned()));
    let request = CreatableTopic::default()
        .with_name(topic_name)
        .with_num_partitions(partitions)
        .with_replication_factor(1);
    let response = client
        .admin()
        .create_topics(
            vec![request],
            CreateTopicsOptions {
                broker_operation_timeout: Some(TOPIC_OP_TIMEOUT),
                wait_for_propagation: true,
            },
            CallOptions::default(),
        )
        .await?;
    let code = response.topics[0].error_code;
    anyhow::ensure!(
        code == 0 || code == ERR_TOPIC_ALREADY_EXISTS,
        "create_topics for '{name}' returned error code {code}",
    );
    Ok(())
}

/// Delete `name` and wait until every broker stops reporting it.
pub async fn delete_topic(client: &Client, name: &str) -> anyhow::Result<()> {
    let topic_name = TopicName::from(StrBytes::from_string(name.to_owned()));
    let response = client
        .admin()
        .delete_topics(
            vec![topic_name],
            DeleteTopicsOptions {
                broker_operation_timeout: Some(TOPIC_OP_TIMEOUT),
                wait_for_propagation: true,
            },
            CallOptions::default(),
        )
        .await?;
    let code = response.responses[0].error_code;
    anyhow::ensure!(
        code == 0 || code == ERR_UNKNOWN_TOPIC_OR_PARTITION,
        "delete_topics for '{name}' returned error code {code}",
    );
    Ok(())
}
