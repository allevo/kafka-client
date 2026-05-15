use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
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
use crate::producer::batch::{RecordPayload, estimate_record_size};
use crate::producer::idempotent::IdempotentState;
use crate::producer::partitioner::{StickyState, partition_for};

pub(crate) mod accumulator;
pub(crate) mod batch;
pub(crate) mod batch_rewrite;
pub(crate) mod idempotent;
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

/// What [`Producer::send`] does when a record cannot reserve
/// `buffer_memory` budget because the accumulator is already holding
/// `buffer_memory` bytes the sender has not drained yet.
///
/// This governs *only* the buffer-memory reservation. `send` can still
/// block up to `max_block` resolving topic metadata regardless of this
/// setting — `Error` does not make `send` unconditionally non-blocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferFullPolicy {
    /// Park the `send` until the sender drains enough buffered batches
    /// to make room, then fail with [`Error::BufferFull`] if the
    /// `max_block` deadline passes first. Mirrors Java's `buffer.memory`
    /// + `max.block.ms` behaviour. The default.
    Block,
    /// Fail the `send` immediately with [`Error::BufferFull`] the moment
    /// the reservation does not fit — never park waiting for room.
    /// Mirrors librdkafka's default `queue.buffering.max.*` →
    /// `RD_KAFKA_RESP_ERR__QUEUE_FULL`, for callers that would rather
    /// shed load synchronously than wait. (Java has no dedicated knob:
    /// `max.block.ms = 0` is its closest analog, but our `max_block` is
    /// shared with the metadata wait, so a separate policy is needed to
    /// avoid coupling the two.)
    Error,
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
    /// Enable the idempotent producer (KIP-98). When `true`, the producer
    /// acquires a `(producer_id, producer_epoch)` from the broker on first
    /// send, assigns monotonic per-partition `base_sequence` numbers, and
    /// retries retryable errors with the same sequence so the broker dedups
    /// any wire-level duplicates.
    ///
    /// Coerces `acks = All`, caps `max_in_flight_per_broker` at 5, and
    /// raises `retries` to `u32::MAX` if it was 0. `acks = None` combined
    /// with idempotence is rejected by [`Producer::new`] (matching Java).
    pub enable_idempotence: bool,
    /// Wall-clock budget for a single record from enqueue to ack. Once
    /// exceeded, the in-flight batch fails with `Error::RequestTimeout`.
    /// Only consulted when `enable_idempotence = true`.
    pub delivery_timeout: Duration,
    /// Hard cap on the encoded size of a single record-batch frame, in
    /// bytes. A frozen batch larger than this is rejected before it
    /// reaches the broker (which would itself answer `MESSAGE_TOO_LARGE`).
    /// Mirrors Java's `max.request.size`. [`Producer::new`] coerces
    /// `batch_size_bytes` down to this value if it was set higher.
    /// Default 1 MiB.
    pub max_request_size: usize,
    /// Hard cap on the estimated size of a single record. [`Producer::send`]
    /// fast-fails with [`Error::RecordTooLarge`] before buffering a record
    /// whose estimated wire size exceeds this. Must not exceed
    /// `max_request_size` ([`Producer::new`] rejects the combination).
    /// Defaults to `max_request_size` (1 MiB).
    pub max_record_size: usize,
    /// Total bytes of un-drained record payloads the accumulator may hold
    /// before [`Producer::send`] applies backpressure. Once the buffer is
    /// full, `send` parks (up to `max_block`) until the sender drains
    /// enough batches to make room, then fails with [`Error::BufferFull`].
    /// Mirrors Java's `buffer.memory`. Must be at least `max_record_size`
    /// ([`Producer::new`] rejects the combination — a record larger than
    /// the whole buffer could never be reserved). Default 32 MiB.
    pub buffer_memory: usize,
    /// What [`Producer::send`] does when a record cannot reserve
    /// `buffer_memory` budget. [`BufferFullPolicy::Block`] (default)
    /// parks the call up to `max_block`; [`BufferFullPolicy::Error`]
    /// fails it synchronously with [`Error::BufferFull`] without
    /// waiting. See [`BufferFullPolicy`].
    pub buffer_full_policy: BufferFullPolicy,
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
            enable_idempotence: false,
            delivery_timeout: Duration::from_secs(120),
            max_request_size: 1 << 20, // 1 MiB
            max_record_size: 1 << 20,  // = max_request_size
            buffer_memory: 32 << 20,   // 32 MiB
            buffer_full_policy: BufferFullPolicy::Block,
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

    /// Toggle the idempotent producer. See
    /// [`ProducerConfig::enable_idempotence`] for the auto-coercion rules
    /// applied at [`Producer::new`] time.
    pub fn with_enable_idempotence(mut self, enable: bool) -> Self {
        self.enable_idempotence = enable;
        self
    }

    /// Set the wall-clock delivery deadline for an idempotent record.
    /// See [`ProducerConfig::delivery_timeout`].
    pub fn with_delivery_timeout(mut self, d: Duration) -> Self {
        self.delivery_timeout = d;
        self
    }

    /// Set the hard cap on a single encoded record-batch frame, in
    /// bytes. See [`ProducerConfig::max_request_size`].
    pub fn with_max_request_size(mut self, bytes: usize) -> Self {
        self.max_request_size = bytes;
        self
    }

    /// Set the hard cap on a single record's estimated wire size, in
    /// bytes. See [`ProducerConfig::max_record_size`].
    pub fn with_max_record_size(mut self, bytes: usize) -> Self {
        self.max_record_size = bytes;
        self
    }

    /// Set the total accumulator buffer budget, in bytes. See
    /// [`ProducerConfig::buffer_memory`].
    pub fn with_buffer_memory(mut self, bytes: usize) -> Self {
        self.buffer_memory = bytes;
        self
    }

    /// Set the behaviour when `buffer_memory` is exhausted. See
    /// [`BufferFullPolicy`].
    pub fn with_buffer_full_policy(mut self, policy: BufferFullPolicy) -> Self {
        self.buffer_full_policy = policy;
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
        if let Some(sleep) = this.sleep.as_mut()
            && sleep.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Err(Error::RequestTimeout(
                "producer send timeout elapsed before broker ack".into(),
            )));
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
    /// Cached from `ProducerConfig::max_record_size`: `send`'s
    /// synchronous oversized-record fast-fail consults it before
    /// reserving any `buffer_memory` budget.
    max_record_size: usize,
    // `Option<_>` so `close(self)` can take the sender / handle exactly
    // once and `Drop` can no-op when close already ran. No mutex needed:
    // `close(self)` has owned access and `Drop` has `&mut self`.
    shutdown_tx: Option<oneshot::Sender<()>>,
    sender_handle: Option<JoinHandle<()>>,
    /// Snapshot of the effective config after auto-coercion, exposed only
    /// to the in-tree tests so they can verify the coercion rules without
    /// rebuilding a producer.
    #[cfg(test)]
    effective_config: Arc<ProducerConfig>,
}

impl Producer {
    /// Test-only: peek at the effective `ProducerConfig` after the
    /// `enable_idempotence` coercion has been applied.
    #[cfg(test)]
    pub(crate) fn effective_config(&self) -> &ProducerConfig {
        &self.effective_config
    }
}

impl Producer {
    /// Spawn the background sender task and return a usable producer
    /// handle. The caller must already be inside a tokio runtime.
    ///
    /// Fails with [`Error::Config`] when `enable_idempotence = true` is
    /// combined with `acks = None` (matches Java's
    /// `enable.idempotence` validation in `KafkaProducer.configureAcks`).
    pub fn new(client: Client, mut config: ProducerConfig) -> Result<Self> {
        if config.enable_idempotence {
            // Java rejects this combo because the broker can't dedup an ack=0
            // produce — there is no response carrying the sequence ack.
            if matches!(config.acks, Acks::None) {
                return Err(Error::Config(
                    "enable_idempotence=true requires acks!=None".into(),
                ));
            }
            // Coerce the same knobs the Java client coerces in
            // ProducerConfig.maybeOverrideEnableIdempotence so users get a
            // consistent guarantee regardless of how they built the config.
            if !matches!(config.acks, Acks::All) {
                tracing::info!("enable_idempotence=true coerces acks to All");
                config.acks = Acks::All;
            }
            if config.max_in_flight_per_broker > 5 {
                tracing::info!(
                    previous = config.max_in_flight_per_broker,
                    "enable_idempotence=true caps max_in_flight_per_broker at 5",
                );
                config.max_in_flight_per_broker = 5;
            }
            if config.retries == 0 {
                tracing::info!("enable_idempotence=true raises retries from 0 to u32::MAX",);
                config.retries = u32::MAX;
            }
        }

        // A record larger than the request cap could never be shipped in
        // any batch — reject the combination up front rather than letting
        // every `send` of a max-size record fail later.
        if config.max_record_size > config.max_request_size {
            return Err(Error::Config(format!(
                "max_record_size ({}) must not exceed max_request_size ({})",
                config.max_record_size, config.max_request_size,
            )));
        }
        // A record larger than the whole buffer could never be reserved
        // against `buffer_memory`: `Producer::send` would park in
        // `reserve` until `max_block` and then surface `BufferFull` for
        // every such record. Reject the combination up front — mirrors
        // Java's `BufferPool.allocate` hard-limit check.
        if config.max_record_size > config.buffer_memory {
            return Err(Error::Config(format!(
                "max_record_size ({}) must not exceed buffer_memory ({})",
                config.max_record_size, config.buffer_memory,
            )));
        }
        // `batch_size_bytes` is the per-partition fill threshold; a value
        // above `max_request_size` would let `append` keep growing a
        // batch past the size every frozen frame is checked against,
        // guaranteeing a `MessageTooLarge` rejection at freeze. Coerce it
        // down (Java clamps the same way in `RecordAccumulator`).
        if config.batch_size_bytes > config.max_request_size {
            tracing::warn!(
                batch_size_bytes = config.batch_size_bytes,
                max_request_size = config.max_request_size,
                "batch_size_bytes exceeds max_request_size; coercing batch_size_bytes down to max_request_size",
            );
            config.batch_size_bytes = config.max_request_size;
        }

        let max_block = config.max_block;
        let max_record_size = config.max_record_size;
        let enable_idempotence = config.enable_idempotence;
        let config = Arc::new(config);
        let (mut acc, ready_rx) = Accumulator::new(config.clone());
        let sticky = Arc::new(StickyState::new());
        let idempotent = enable_idempotence.then(|| Arc::new(IdempotentState::new()));
        // T2c: let the accumulator's `reserve` see the idempotent
        // in-flight queue, so a stalled broker can't hoard `buffer_memory`
        // through retried batches that have already left `bytes_in_use`.
        if let Some(idem) = &idempotent {
            acc.attach_idempotent(idem.clone());
        }
        let accumulator = Arc::new(acc);

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handle = sender::spawn_sender(
            client.clone(),
            accumulator.clone(),
            config.clone(),
            idempotent.clone(),
            ready_rx,
            shutdown_rx,
        );

        Ok(Self {
            client,
            accumulator,
            sticky,
            max_block,
            max_record_size,
            shutdown_tx: Some(shutdown_tx),
            sender_handle: Some(handle),
            #[cfg(test)]
            effective_config: config,
        })
    }

    /// Enqueue `record` for delivery. Resolves once the record is
    /// appended to the accumulator (after a `buffer_memory` reservation
    /// and a metadata refresh if needed). Awaiting the returned
    /// [`SendFuture`] then waits for the broker ack.
    ///
    /// `timeout` is a whole-broker-round-trip budget.
    /// `None` falls back to per-stage bounds (`max_block` for the
    /// combined buffer-reservation + metadata wait, the sender's
    /// per-attempt timeouts for the dispatch path).
    pub async fn send(
        &self,
        record: ProducerRecord,
        timeout: Option<Duration>,
    ) -> Result<SendFuture> {
        let now = tokio::time::Instant::now();
        // The caller's `timeout` is the whole-round-trip budget; it also
        // arms the post-enqueue ack wait via `SendFuture::sleep`.
        let user_deadline = timeout.map(|t| now + t);
        // N4: the `buffer_memory` reservation wait and the
        // metadata-resolution wait share ONE budget — `max_block`,
        // capped by `timeout` when that is tighter. Computed once here
        // so the two stages cannot each spend a fresh `max_block`.
        let block_deadline = {
            let max_block_deadline = now + self.max_block;
            match user_deadline {
                Some(u) => u.min(max_block_deadline),
                None => max_block_deadline,
            }
        };

        // The record's estimated wire footprint is both the
        // `max_record_size` guard input and the `buffer_memory` amount
        // to reserve — taken once, off the `ProducerRecord` (no
        // `RecordPayload` exists yet).
        let est = estimate_record_size(record.key.as_ref(), record.value.as_ref(), &record.headers);
        // Fast-fail an oversized record before touching the buffer
        // budget: it can never ship, so reserving (then releasing)
        // budget for it is pointless — and a record larger than
        // `buffer_memory` itself would otherwise park in `reserve` until
        // `max_block` only to surface a misleading `BufferFull`.
        // `Accumulator::append` re-checks; this is the user-facing fast
        // path.
        if est > self.max_record_size {
            return Err(Error::RecordTooLarge {
                size: est,
                limit: self.max_record_size,
            });
        }

        // Reserve `buffer_memory` budget first (N4: shares
        // `block_deadline` with the metadata wait below). From here
        // until `append` succeeds, every early return MUST
        // `release(est)` or the reserved budget leaks — the record never
        // reaches a `PartitionBatch`, so no drain would ever release it.
        self.accumulator.reserve(est, block_deadline).await?;

        let meta = match self
            .resolve_topic_metadata(&record.topic, Some(block_deadline))
            .await
        {
            Ok(m) => m,
            Err(e) => {
                self.accumulator.release(est);
                return Err(e);
            }
        };

        let Some(partition) = partition_for(&record, &meta, &self.sticky) else {
            self.accumulator.release(est);
            return Err(Error::NoBrokerAvailable(format!(
                "no partitions available for topic '{}'",
                record.topic.as_str()
            )));
        };

        // Capture the sticky-routing inputs before `record` is consumed
        // into `payload`: a record with neither an explicit partition nor
        // a key took the sticky branch in `partition_for`, the same
        // condition checked here.
        let used_sticky = record.partition.is_none() && record.key.is_none();
        let num_partitions = i32::try_from(meta.partitions.len()).ok();

        let topic = record.topic;
        let payload = RecordPayload {
            key: record.key,
            value: record.value,
            timestamp: record.timestamp.unwrap_or_else(now_ms),
            headers: record.headers,
        };

        let outcome = match self.accumulator.append(topic.clone(), partition, payload) {
            Ok(o) => o,
            Err(e) => {
                self.accumulator.release(est);
                return Err(e);
            }
        };

        // KIP-480 sticky partitioning: once the sticky batch fills, the
        // record just appended sealed it — advance the sticky partition so
        // the next keyless record spreads to a fresh partition instead of
        // re-pinning partition 0. Two concurrent keyless `send`s can both
        // observe `is_full` and rotate twice (skipping a partition);
        // records still spread, so we don't lock across the whole
        // `partition_for` + `append` + `rotate` span for that.
        if used_sticky
            && outcome.is_full
            && let Some(n) = num_partitions
        {
            self.sticky.rotate(&topic, n);
        }

        let sleep = user_deadline.map(|d| Box::pin(tokio::time::sleep_until(d)));
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
            match tokio::time::timeout_at(
                deadline,
                self.client.refresh_topics(std::slice::from_ref(topic)),
            )
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
        self.accumulator.await_empty().await;
        Ok(())
    }

    /// Signal the sender task to drain the accumulator one last time
    /// and exit, then await its completion.
    pub async fn close(mut self) -> Result<()> {
        // Wake any `Producer::send` parked on the buffer cap so it
        // returns with `Error::Protocol` instead of waiting out
        // `max_block` (N9).
        self.accumulator.mark_closed();
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.sender_handle.take() {
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
        if let Some(tx) = self.shutdown_tx.take() {
            tracing::warn!("producer dropped without close()");
            // Wake any send parked on the buffer cap (N9) — same reason
            // as close(), Drop just can't await the sender afterwards.
            self.accumulator.mark_closed();
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
        assert!(!cfg.enable_idempotence);
        assert_eq!(cfg.delivery_timeout, Duration::from_secs(120));
        assert_eq!(cfg.max_request_size, 1 << 20);
        assert_eq!(cfg.max_record_size, 1 << 20);
        assert_eq!(cfg.buffer_memory, 32 << 20);
        assert_eq!(cfg.buffer_full_policy, BufferFullPolicy::Block);
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
            .with_max_block(Duration::from_secs(5))
            .with_enable_idempotence(true)
            .with_delivery_timeout(Duration::from_secs(7))
            .with_max_request_size(2 << 20)
            .with_max_record_size(512 * 1024)
            .with_buffer_memory(8 << 20)
            .with_buffer_full_policy(BufferFullPolicy::Error);

        assert_eq!(cfg.acks, Acks::Leader);
        assert_eq!(cfg.linger, Duration::from_millis(20));
        assert_eq!(cfg.batch_size_bytes, 64 * 1024);
        assert_eq!(cfg.compression, Compression::Gzip);
        assert_eq!(cfg.max_in_flight_per_broker, 1);
        assert_eq!(cfg.retries, 3);
        assert_eq!(cfg.max_block, Duration::from_secs(5));
        assert!(cfg.enable_idempotence);
        assert_eq!(cfg.delivery_timeout, Duration::from_secs(7));
        assert_eq!(cfg.max_request_size, 2 << 20);
        assert_eq!(cfg.max_record_size, 512 * 1024);
        assert_eq!(cfg.buffer_memory, 8 << 20);
        assert_eq!(cfg.buffer_full_policy, BufferFullPolicy::Error);
    }
}
