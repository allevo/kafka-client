use std::collections::HashMap;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
use kafka_protocol::messages::{
    ApiKey, BrokerId, ListOffsetsRequest, ListOffsetsResponse, TopicName,
};

use crate::client::{CallOptions, Client, NodeTarget, PartitionId};
use crate::consumer::{IsolationLevel, OffsetReset, TopicPartition};
use crate::error::{Error, Result};

// ListOffsets sentinel timestamps (Kafka protocol):
//   -2 → Earliest (start of the log)
//   -1 → Latest   (high-water mark / LSO under READ_COMMITTED)
const TIMESTAMP_EARLIEST: i64 = -2;
const TIMESTAMP_LATEST: i64 = -1;

// `replica_id = -1` is the canonical "regular consumer" marker on
// ListOffsets / Fetch; broker / follower replicas use their real id.
const CONSUMER_REPLICA_ID: i32 = -1;

/// Resolve the start offset for every `(topic, partition)` in `topics`
/// hosted by `leader_id`, using the broker's view of the log per
/// `reset`.
///
/// All partitions in `topics` must share the same leader — the caller is
/// expected to group by leader before invoking this. The returned map is
/// keyed by `TopicPartition` and contains exactly one entry per requested
/// partition; a missing or errored partition surfaces as `Err` so the
/// caller can treat assignment seeding as all-or-nothing.
pub(super) async fn resolve_start_offsets(
    client: &Client,
    leader_id: BrokerId,
    isolation: IsolationLevel,
    topics: &[(TopicName, Vec<PartitionId>)],
    reset: OffsetReset,
) -> Result<HashMap<TopicPartition, i64>> {
    if topics.is_empty() {
        return Ok(HashMap::new());
    }

    let timestamp = match reset {
        OffsetReset::Earliest => TIMESTAMP_EARLIEST,
        OffsetReset::Latest => TIMESTAMP_LATEST,
    };

    let request_topics: Vec<ListOffsetsTopic> = topics
        .iter()
        .map(|(name, partitions)| {
            let parts: Vec<ListOffsetsPartition> = partitions
                .iter()
                .map(|p| {
                    ListOffsetsPartition::default()
                        .with_partition_index(p.0)
                        // `-1` → "I don't know the epoch, don't fence me".
                        // Slice 1 doesn't track per-partition epochs at the
                        // resolve-offset call site; the fetcher loop is the
                        // place where epoch fencing matters.
                        .with_current_leader_epoch(-1)
                        .with_timestamp(timestamp)
                })
                .collect();
            ListOffsetsTopic::default()
                .with_name(name.clone())
                .with_partitions(parts)
        })
        .collect();

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(CONSUMER_REPLICA_ID))
        .with_isolation_level(isolation as i8)
        .with_topics(request_topics);

    let response: ListOffsetsResponse = client
        .send(
            NodeTarget::Broker(leader_id),
            ApiKey::ListOffsets,
            8,
            request,
            &CallOptions::default(),
        )
        .await?;

    let mut out: HashMap<TopicPartition, i64> = HashMap::new();
    for topic_resp in &response.topics {
        for partition_resp in &topic_resp.partitions {
            if partition_resp.error_code != 0 {
                let response_error = ResponseError::try_from_code(partition_resp.error_code)
                    .unwrap_or(ResponseError::Unknown(partition_resp.error_code));
                return Err(Error::Broker {
                    error: response_error,
                });
            }
            out.insert(
                TopicPartition {
                    topic: topic_resp.name.clone(),
                    partition: PartitionId(partition_resp.partition_index),
                },
                partition_resp.offset,
            );
        }
    }

    // Verify the broker echoed back every partition we asked about — a
    // missing entry would silently leave a cursor un-seeded later.
    for (name, partitions) in topics {
        for p in partitions {
            let key = TopicPartition {
                topic: name.clone(),
                partition: *p,
            };
            if !out.contains_key(&key) {
                return Err(Error::Protocol(format!(
                    "ListOffsets response missing partition {}-{}",
                    name.as_str(),
                    p.0,
                )));
            }
        }
    }

    Ok(out)
}
