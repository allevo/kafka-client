use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::client::{Client, PartitionId, TopicMetadata};
use crate::error::{Error, Result, RetryAction};
use crate::producer::accumulator::Accumulator;
use crate::producer::batch::RecordPayload;
use crate::producer::partitioner::{StickyState, partition_for};

pub(crate) mod accumulator;
pub(crate) mod batch;
pub(crate) mod partitioner;
pub(crate) mod sender;

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
    /// Record headers.
    ///
    /// NB: Inserting the same name twice keeps only the last value.
    /// Even if the Kafka v2 wire format permits repeated header names, the
    /// `kafka_protocol` crate stores them in an `IndexMap`, which collapses
    /// them.
    pub headers: IndexMap<StrBytes, Option<Bytes>>,
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
            headers: IndexMap::new(),
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

    /// Insert a header.
    pub fn with_header(mut self, name: StrBytes, value: Option<Bytes>) -> Self {
        self.headers.insert(name, value);
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

impl Acks {
    fn as_i16(&self) -> i16 {
        match self {
            Acks::None => 0,
            Acks::Leader => 1,
            Acks::All => -1,
        }
    }
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
    /// Maximum time [`Producer::send`] will block waiting for cluster
    /// metadata to resolve a record's topic.
    pub max_block: Duration,
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
            max_block: Duration::from_secs(60),
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

    /// Set how long [`Producer::send`] will block waiting for topic
    /// metadata to resolve. See [`ProducerConfig::max_block`].
    pub fn with_max_block(mut self, max_block: Duration) -> Self {
        self.max_block = max_block;
        self
    }
}

/// Resolves to the broker's acknowledgement for a single record. The
/// outer `async` value returned by [`Producer::send`] resolves once the
/// record is enqueued; awaiting this `SendFuture` then waits for the
/// broker to ack the produce request.
///
/// When [`Producer::send`] is called with a `timeout`, the same wall-clock
/// deadline armed at enqueue-time is also enforced here.
pub struct SendFuture {
    rx: oneshot::Receiver<Result<RecordMetadata>>,
    // `Pin<Box<Sleep>>` so the whole struct stays `Unpin` — `Sleep` is
    // `!Unpin` on its own. `None` when `Producer::send` was called
    // without a timeout.
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Future for SendFuture {
    type Output = Result<RecordMetadata>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Deadline check first so a fired timer still wins even if the
        // ack lands in the same poll — preserves the wall-clock contract
        // the caller asked for when they passed `Some(timeout)`.
        if let Some(sleep) = this.sleep.as_mut() {
            if sleep.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(Error::RequestTimeout(
                    "producer send timeout elapsed before broker ack".into(),
                )));
            }
        }
        match Pin::new(&mut this.rx).poll(cx) {
            Poll::Ready(Ok(r)) => Poll::Ready(r),
            // RecvError means the oneshot sender was dropped without a
            // value — the sender task exited (close/drop) before this
            // batch's waiter was notified.
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Protocol("producer closed".into()))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Asynchronous Kafka producer. Records appended via [`Producer::send`]
/// are buffered per (topic, partition) and dispatched by a background
/// sender task that batches them into `ProduceRequest`s.
pub struct Producer {
    client: Client,
    accumulator: Arc<Accumulator>,
    sticky: Arc<StickyState>,
    /// Cached from `ProducerConfig::max_block` so `send` doesn't pay the
    /// `Arc<ProducerConfig>` deref on every call.
    max_block: Duration,
    // `Mutex<Option<_>>` so `close(self)` can take the sender / handle
    // exactly once and `Drop` can no-op when close already ran.
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    sender_handle: Mutex<Option<JoinHandle<()>>>,
}

impl Producer {
    /// Spawn the background sender task and return a usable producer
    /// handle. The caller must already be inside a tokio runtime.
    pub fn new(client: Client, config: ProducerConfig) -> Self {
        let max_block = config.max_block;
        let config = Arc::new(config);
        let (acc, ready_rx) = Accumulator::new(config.clone());
        let accumulator = Arc::new(acc);
        let sticky = Arc::new(StickyState::new());

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = sender::spawn_sender(
            client.clone(),
            accumulator.clone(),
            config.clone(),
            ready_rx,
            shutdown_rx,
        );

        Self {
            client,
            accumulator,
            sticky,
            max_block,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            sender_handle: Mutex::new(Some(handle)),
        }
    }

    /// Enqueue `record` for delivery. Resolves once the record is
    /// appended to the accumulator (after a metadata refresh if
    /// needed). Awaiting the returned [`SendFuture`] then waits for the
    /// broker ack.
    ///
    /// `timeout` is a whole-broker-round-trip budget.
    /// `None` falls back to per-stage bounds (`max_block` for metadata,
    /// the sender's per-attempt timeouts for the dispatch path).
    pub async fn send(
        &self,
        record: ProducerRecord,
        timeout: Option<Duration>,
    ) -> Result<SendFuture> {
        let deadline = timeout.map(|t| tokio::time::Instant::now() + t);
        let meta = self.resolve_topic_metadata(&record.topic, deadline).await?;

        let partition = partition_for(&record, &meta, &self.sticky).ok_or_else(|| {
            Error::NoBrokerAvailable(format!(
                "no partitions available for topic '{}'",
                record.topic.as_str()
            ))
        })?;

        let topic = record.topic;
        let payload = RecordPayload {
            key: record.key,
            value: record.value,
            timestamp: record.timestamp.unwrap_or_else(now_ms),
            headers: record.headers,
        };

        let outcome = self.accumulator.append(topic, partition, payload);
        let sleep = deadline.map(|d| Box::pin(tokio::time::sleep_until(d)));
        Ok(SendFuture {
            rx: outcome.rx,
            sleep,
        })
    }

    /// Resolve `topic` to a usable `TopicMetadata` (no error, partitions
    /// known) by re-issuing `MetadataRequest`s until the topic resolves,
    /// a non-retriable error appears, or the active deadline elapses.
    ///
    /// `InvalidMetadataException`-family codes (UNKNOWN_TOPIC_OR_PARTITION,
    /// LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER, KAFKA_STORAGE_ERROR,
    /// …) trigger another refresh; permanent codes
    /// (TOPIC_AUTHORIZATION_FAILED, INVALID_TOPIC_EXCEPTION) bail out
    /// immediately.
    async fn resolve_topic_metadata(
        &self,
        topic: &TopicName,
        global_deadline: Option<tokio::time::Instant>,
    ) -> Result<TopicMetadata> {
        let start = tokio::time::Instant::now();
        let max_block_deadline = start + self.max_block;
        let deadline = match global_deadline {
            Some(g) => g.min(max_block_deadline),
            None => max_block_deadline,
        };
        // Report whichever budget actually applied so the caller can
        // tell a tight `timeout` from `max_block` in the error message.
        let budget = deadline.saturating_duration_since(start);
        let mut last_error: Option<ResponseError> = None;

        loop {
            let cached = self.client.topic_metadata(topic);

            match cached {
                // Healthy cached entry — done.
                Some(m) if m.error.is_none() => return Ok(m),
                // Cached with a non-refreshable error — fail fast so the
                // caller sees the real cause (auth failure, invalid topic
                // name) instead of looping uselessly.
                Some(ref m)
                    if m.error.is_some_and(|e| {
                        !matches!(
                            Error::Broker { error: e }.classify(),
                            RetryAction::RefreshMetadata,
                        )
                    }) =>
                {
                    return Err(Error::Broker {
                        // safe: matched guard guarantees Some
                        error: m.error.unwrap_or(ResponseError::UnknownServerError),
                    });
                }
                Some(m) => {
                    // Refresh-worthy error: remember it for the timeout
                    // message, then loop to refresh.
                    last_error = m.error;
                }
                None => { /* Never seen; refresh below. */ }
            }

            // Budget check: any remaining time for another refresh?
            // Picking up `now` after the cache check above means the
            // healthy / fail-fast paths still fire even with a zero
            // budget; only cases that would actually wait are gated.
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Err(Error::RequestTimeout(timeout_message(
                    topic, budget, last_error,
                )));
            }

            // Bound the refresh to whatever is left of the budget so a
            // hung broker can't extend the wait beyond `deadline`.
            match tokio::time::timeout_at(deadline, self.client.refresh_topics(&[topic.clone()]))
                .await
            {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(Error::RequestTimeout(timeout_message(
                        topic, budget, last_error,
                    )));
                }
            }
        }
    }

    /// Wait until every record currently in the accumulator has been
    /// drained.
    pub async fn flush(&self) -> Result<()> {
        while !self.accumulator.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    /// Signal the sender task to drain the accumulator one last time
    /// and exit, then await its completion.
    pub async fn close(self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        let handle = self.sender_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            // JoinError only fires on panic / cancellation; either way
            // we've already signalled the task, so swallow it.
            let _ = handle.await;
        }
        Ok(())
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        // Producer dropped without close(): signal shutdown so the
        // sender task exits, but don't await — Drop is sync. Pending
        // SendFutures whose waiters still live in the accumulator
        // resolve to `Error::Protocol("producer closed")` once the
        // sender exits and drops the FrozenBatch waiters.
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            tracing::warn!("producer dropped without close()");
            let _ = tx.send(());
            // sender_handle deliberately leaked: Drop can't await it.
        }
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Format the `max_block` timeout message, threading the last
/// observed broker-side error code into the string when one is known.
fn timeout_message(topic: &TopicName, budget: Duration, last: Option<ResponseError>) -> String {
    match last {
        Some(e) => format!(
            "metadata for topic '{}' did not resolve within {:?} (last error: {})",
            topic.as_str(),
            budget,
            e,
        ),
        None => format!(
            "metadata for topic '{}' did not resolve within {:?}",
            topic.as_str(),
            budget,
        ),
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
            .with_header(header_name.clone(), Some(header_value.clone()));

        assert_eq!(rec.topic, topic);
        assert_eq!(rec.key, Some(key));
        assert_eq!(rec.value, Some(value));
        assert_eq!(rec.partition, Some(PartitionId(7)));
        assert_eq!(rec.timestamp, Some(42));
        assert_eq!(rec.headers.len(), 1);
        assert_eq!(rec.headers.get(&header_name), Some(&Some(header_value)));
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
        assert_eq!(cfg.max_block, Duration::from_secs(60));
    }

    #[test]
    fn producer_config_builders_override_defaults() {
        let cfg = ProducerConfig::new()
            .with_acks(Acks::Leader)
            .with_linger(Duration::from_millis(20))
            .with_batch_size_bytes(64 * 1024)
            .with_compression(Compression::Gzip)
            .with_max_in_flight_per_broker(1)
            .with_retries(3)
            .with_max_block(Duration::from_secs(5));

        assert_eq!(cfg.acks, Acks::Leader);
        assert_eq!(cfg.linger, Duration::from_millis(20));
        assert_eq!(cfg.batch_size_bytes, 64 * 1024);
        assert_eq!(cfg.compression, Compression::Gzip);
        assert_eq!(cfg.max_in_flight_per_broker, 1);
        assert_eq!(cfg.retries, 3);
        assert_eq!(cfg.max_block, Duration::from_secs(5));
    }
}
