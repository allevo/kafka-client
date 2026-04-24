use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;

use crate::client::PartitionId;

pub(crate) mod partitioner;

/// A record to be published to a Kafka topic.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    /// Target topic.
    pub topic: TopicName,
    /// Explicit destination partition. When `None`, the partitioner picks one
    /// from the topic's available partitions.
    pub partition: Option<PartitionId>,
    /// Optional record key. The default partitioner hashes this to route all
    /// records with the same key to the same partition.
    pub key: Option<Bytes>,
    /// Record payload.
    pub value: Option<Bytes>,
    /// Producer-assigned timestamp in milliseconds since the Unix epoch. When
    /// `None`, the value used depends on the topic's `message.timestamp.type`
    /// (`CreateTime` vs. `LogAppendTime`).
    pub timestamp: Option<i64>,
    /// Record headers, in insertion order. Names are not required to be unique.
    pub headers: Vec<(StrBytes, Bytes)>,
}

impl ProducerRecord {
    /// Create a record targeting `topic` with the given payload and no key.
    pub fn new(topic: TopicName, value: Bytes) -> Self {
        Self {
            topic,
            partition: None,
            key: None,
            value: Some(value),
            timestamp: None,
            headers: Vec::new(),
        }
    }

    /// Set the record key, used by the default partitioner for routing.
    pub fn with_key(mut self, key: Bytes) -> Self {
        self.key = Some(key);
        self
    }

    /// Pin the record to a specific partition, bypassing the partitioner.
    pub fn with_partition(mut self, partition: PartitionId) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set the record timestamp in milliseconds since the Unix epoch.
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Append a header. Headers preserve insertion order and may repeat names.
    pub fn with_header(mut self, name: StrBytes, value: Bytes) -> Self {
        self.headers.push((name, value));
        self
    }
}

/// Acknowledgement returned by the broker when a record has been persisted.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RecordMetadata {
    /// Topic the record was written to.
    pub topic: TopicName,
    /// Partition the record was assigned to.
    pub partition: PartitionId,
    /// Offset of the record within the partition.
    pub offset: i64,
    /// Timestamp persisted with the record. `None` when the broker returned no
    /// append-time value (e.g. older protocol versions or `acks = None`).
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    None,
    Leader,
    All,
}

/// Tunables for the producer. Use [`ProducerConfig::default`] for reasonable
/// starting values and refine via the `with_*` builders.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Number of replica acknowledgements the broker must receive before
    /// replying to the producer.
    pub acks: Acks,
    /// How long the producer waits for additional records before sending a
    /// partially filled batch.
    pub linger: Duration,
    /// Upper bound on the size of a single record batch, per partition. A
    /// batch is sent as soon as it fills, regardless of `linger`.
    pub batch_size_bytes: usize,
    /// Compression codec applied to outgoing record batches.
    pub compression: Compression,
    /// Maximum unacknowledged produce requests in flight per broker
    /// connection. Set to `1` to preserve ordering across retries while
    /// `retries > 0`.
    pub max_in_flight_per_broker: usize,
    /// Number of times to resend a failed batch. Default `0`.
    ///
    /// Raising this above zero while `max_in_flight_per_broker > 1` silently
    /// reorders records on retry until v2 idempotence lands. Set
    /// `max_in_flight_per_broker = 1` if you raise this.
    pub retries: u32,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            acks: Acks::All,
            linger: Duration::from_millis(5),
            batch_size_bytes: 16 * 1024, // 16 kB
            compression: Compression::None,
            max_in_flight_per_broker: 5,
            retries: 0,
        }
    }
}

impl ProducerConfig {
    /// Create a config pre-populated with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the acknowledgement level required from the broker.
    pub fn with_acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    /// Set how long to wait for more records before flushing a batch.
    pub fn with_linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    /// Set the maximum size of a per-partition record batch, in bytes.
    pub fn with_batch_size_bytes(mut self, bytes: usize) -> Self {
        self.batch_size_bytes = bytes;
        self
    }

    /// Set the compression codec for outgoing batches.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Set the maximum number of unacknowledged produce requests per broker.
    pub fn with_max_in_flight_per_broker(mut self, n: usize) -> Self {
        self.max_in_flight_per_broker = n;
        self
    }

    /// Set the number of retry attempts for failed batches. See the field
    /// docs on [`ProducerConfig::retries`] for the ordering caveat.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_record_builders_compose() {
        let topic = TopicName(StrBytes::from_static_str("orders"));
        let key = Bytes::from_static(b"k1");
        let value = Bytes::from_static(b"v1");
        let header_name = StrBytes::from_static_str("trace-id");
        let header_value = Bytes::from_static(b"abc123");

        let rec = ProducerRecord::new(topic.clone(), value.clone())
            .with_key(key.clone())
            .with_partition(PartitionId(7))
            .with_timestamp(42)
            .with_header(header_name.clone(), header_value.clone());

        assert_eq!(rec.topic, topic);
        assert_eq!(rec.key, Some(key));
        assert_eq!(rec.value, Some(value));
        assert_eq!(rec.partition, Some(PartitionId(7)));
        assert_eq!(rec.timestamp, Some(42));
        assert_eq!(rec.headers, vec![(header_name, header_value)]);
    }

    #[test]
    fn producer_config_default_matches_documented_values() {
        let cfg = ProducerConfig::default();
        assert_eq!(cfg.acks, Acks::All);
        assert_eq!(cfg.linger, Duration::from_millis(5));
        assert_eq!(cfg.batch_size_bytes, 16 * 1024);
        assert_eq!(cfg.compression, Compression::None);
        assert_eq!(cfg.max_in_flight_per_broker, 5);
        assert_eq!(cfg.retries, 0);
    }

    #[test]
    fn producer_config_builders_override_defaults() {
        let cfg = ProducerConfig::new()
            .with_acks(Acks::Leader)
            .with_linger(Duration::from_millis(20))
            .with_batch_size_bytes(64 * 1024)
            .with_compression(Compression::Gzip)
            .with_max_in_flight_per_broker(1)
            .with_retries(3);

        assert_eq!(cfg.acks, Acks::Leader);
        assert_eq!(cfg.linger, Duration::from_millis(20));
        assert_eq!(cfg.batch_size_bytes, 64 * 1024);
        assert_eq!(cfg.compression, Compression::Gzip);
        assert_eq!(cfg.max_in_flight_per_broker, 1);
        assert_eq!(cfg.retries, 3);
    }
}
