use std::time::{Duration, Instant};

use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use crate::{Consumer, ConsumerRecord, PartitionId, TopicPartition};

/// Build a `TopicPartition` from a static topic name and partition id.
/// Test-only convenience over the verbose
/// `TopicPartition { topic: TopicName::from(StrBytes::from_static_str(...)), partition: ... }`.
pub fn topic_partition(name: &'static str, p: i32) -> TopicPartition {
    TopicPartition {
        topic: TopicName::from(StrBytes::from_static_str(name)),
        partition: PartitionId(p),
    }
}

/// Drain exactly `n` records from `consumer` under an overall deadline.
///
/// Each `recv()` is bounded by the time remaining until `deadline`, so a
/// stuck fetcher fails fast with an assertion-friendly message instead of
/// hanging the test. Returns the records on success; panics on timeout,
/// stream end, or a fetcher-emitted error.
pub async fn collect_n(consumer: &Consumer, n: usize, timeout: Duration) -> Vec<ConsumerRecord> {
    let deadline = Instant::now() + timeout;
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| {
                panic!(
                    "collect_n timed out after {:?} with {}/{} records",
                    timeout,
                    out.len(),
                    n
                )
            });
        match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Some(Ok(record))) => out.push(record),
            Ok(Some(Err(e))) => panic!(
                "consumer yielded error after {}/{} records: {e:?}",
                out.len(),
                n
            ),
            Ok(None) => panic!("consumer stream ended after {}/{} records", out.len(), n),
            Err(_) => panic!(
                "collect_n timed out after {:?} with {}/{} records",
                timeout,
                out.len(),
                n
            ),
        }
    }
    out
}
