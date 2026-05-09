use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::PartitionId;
use crate::client::Client;
use crate::consumer::fetcher::{AssignmentUpdate, spawn_fetcher};
use crate::error::{Error, Result};

mod decode;
mod fetcher;
mod offsets;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: TopicName,
    pub partition: PartitionId,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerRecord {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub offset: i64,
    // `None` when the broker reports `NO_TIMESTAMP` (-1) — the producer didn't
    // attach a CreateTime and the topic isn't in `LogAppendTime` mode.
    pub timestamp: Option<i64>,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    // Mirrors `ProducerRecord::headers`: the v2 wire format allows repeated
    // header names, but `kafka_protocol` collapses them into an `IndexMap`,
    // so duplicates are not preserved on decode.
    pub headers: IndexMap<StrBytes, Option<Bytes>>,
}

// Wire mapping matches the `timestamp` field of ListOffsets: -2 = Earliest, -1 = Latest.
// The discriminants here are the consumer-config representation, not the wire value;
// the translation lives in `offsets.rs`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum OffsetReset {
    Earliest,
    Latest,
}

// Wire mapping per Kafka protocol: cast `as i8` produces the byte sent in
// FetchRequest / ListOffsetsRequest's `isolation_level` field.
//   ReadUncommitted = 0
//   ReadCommitted   = 1
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum IsolationLevel {
    ReadUncommitted = 0,
    ReadCommitted = 1,
}

/// Tunables for the consumer. Use [`ConsumerConfig::default`] for reasonable
/// starting values and refine via the `with_*` builders.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Minimum bytes the broker accumulates before completing a Fetch. Maps
    /// directly to FetchRequest's `min_bytes`.
    pub fetch_min_bytes: i32,
    /// Upper bound on the total bytes returned by a single Fetch across all
    /// partitions. Maps to FetchRequest's `max_bytes`.
    pub fetch_max_bytes: i32,
    /// Per-partition byte cap inside a Fetch. Maps to the per-partition
    /// `partition_max_bytes` field.
    pub partition_max_bytes: i32,
    /// Maximum time the broker will block waiting for `fetch_min_bytes` to
    /// accumulate. Maps to FetchRequest's `max_wait_ms`.
    pub fetch_max_wait: Duration,
    /// Where to start reading when no committed offset exists for a
    /// partition (slice 1: every assignment).
    pub auto_offset_reset: OffsetReset,
    /// Capacity of the internal fetcher → consumer record channel.
    pub channel_capacity: usize,
    /// Maximum time [`Consumer::assign`] will block waiting on metadata
    /// refresh and start-offset resolution.
    pub max_block: Duration,
    /// Read isolation level. Sent as the `isolation_level` byte on Fetch
    /// and ListOffsets requests.
    pub isolation_level: IsolationLevel,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            fetch_min_bytes: 1,
            fetch_max_bytes: 50 * 1024 * 1024, // 50 MiB
            partition_max_bytes: 1024 * 1024,  // 1 MiB
            fetch_max_wait: Duration::from_millis(500),
            auto_offset_reset: OffsetReset::Earliest,
            channel_capacity: 1024,
            max_block: Duration::from_secs(60),
            isolation_level: IsolationLevel::ReadUncommitted,
        }
    }
}

impl ConsumerConfig {
    /// Create a config pre-populated with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the minimum bytes the broker accumulates before completing a Fetch.
    pub fn with_fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.fetch_min_bytes = bytes;
        self
    }

    /// Set the upper bound on the total bytes returned by a single Fetch.
    pub fn with_fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.fetch_max_bytes = bytes;
        self
    }

    /// Set the per-partition byte cap inside a Fetch.
    pub fn with_partition_max_bytes(mut self, bytes: i32) -> Self {
        self.partition_max_bytes = bytes;
        self
    }

    /// Set the maximum time the broker will block waiting for
    /// `fetch_min_bytes` to accumulate.
    pub fn with_fetch_max_wait(mut self, max_wait: Duration) -> Self {
        self.fetch_max_wait = max_wait;
        self
    }

    /// Set where to start reading when no committed offset exists.
    pub fn with_auto_offset_reset(mut self, reset: OffsetReset) -> Self {
        self.auto_offset_reset = reset;
        self
    }

    /// Set the capacity of the internal fetcher → consumer record channel.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Set the maximum time [`Consumer::assign`] will block on metadata and
    /// start-offset resolution.
    pub fn with_max_block(mut self, max_block: Duration) -> Self {
        self.max_block = max_block;
        self
    }

    /// Set the read isolation level.
    pub fn with_isolation_level(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }
}

/// Asynchronous Kafka consumer. Records are streamed from a background
/// fetcher task spawned at construction; callers consume them via
/// [`Consumer::recv`] or by awaiting the [`futures_core::Stream`] impl.
#[allow(dead_code)]
pub struct Consumer {
    client: Client,
    config: Arc<ConsumerConfig>,
    // `mpsc::Receiver::recv` / `poll_recv` already require `&mut`, and
    // `recv` / `Stream::poll_next` are the only readers — exclusive
    // access through `&mut Consumer` is enough. No mutex needed.
    record_rx: mpsc::Receiver<Result<ConsumerRecord>>,
    // Unbounded so `seek` can stay synchronous: a Seek update is tiny,
    // and bounding it would force `seek` to be `async` just to handle
    // back-pressure from the fetcher. `assign` already awaits its own
    // ack channel for completion semantics.
    assign_tx: mpsc::UnboundedSender<AssignmentUpdate>,
    // `Option<_>` so `close(self)` can take the sender / handle exactly
    // once and `Drop` can no-op when close already ran. No mutex needed:
    // `close(self)` has owned access and `Drop` has `&mut self`.
    shutdown_tx: Option<oneshot::Sender<()>>,
    fetcher_handle: Option<JoinHandle<()>>,
}

impl Consumer {
    /// Spawn the background fetcher task and return a usable consumer
    /// handle. The caller must already be inside a tokio runtime.
    /// No broker connections are opened here; metadata refresh and
    /// start-offset resolution are deferred to [`Consumer::assign`].
    pub fn new(client: Client, config: ConsumerConfig) -> Self {
        let config = Arc::new(config);
        let (record_tx, record_rx) = mpsc::channel(config.channel_capacity);
        // Unbounded so `seek` can stay synchronous (see field doc on
        // `assign_tx`); `assign` awaits its own ack channel for
        // completion semantics so unboundedness doesn't silently mask
        // overload.
        let (assign_tx, assign_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = spawn_fetcher(
            client.clone(),
            config.clone(),
            record_tx,
            assign_rx,
            shutdown_rx,
        );

        Self {
            client,
            config,
            record_rx,
            assign_tx,
            shutdown_tx: Some(shutdown_tx),
            fetcher_handle: Some(handle),
        }
    }

    /// Replace the consumer's partition assignment. Returns once the
    /// fetcher has refreshed metadata, resolved start offsets, and
    /// seeded its cursor map, so the first `recv` / `poll_next` after
    /// `assign` is guaranteed to see a fully primed fetcher.
    ///
    /// Bounded by [`ConsumerConfig::max_block`]: only the metadata
    /// refresh is timed out here. The fetcher's own work runs without
    /// an additional bound — its blocking step is the same metadata
    /// call against an already-warm cache, plus a `ListOffsets` per
    /// leader that the broker answers synchronously.
    pub async fn assign(&self, tps: Vec<TopicPartition>) -> Result<()> {
        // Dedup topics so the user-facing metadata refresh sends one
        // MetadataRequestTopic per topic. The fetcher's `handle_replace`
        // also dedups, but for a different purpose (grouping
        // ListOffsets by leader); both are needed.
        let mut unique_topics: Vec<TopicName> = Vec::new();
        for tp in &tps {
            if !unique_topics.contains(&tp.topic) {
                unique_topics.push(tp.topic.clone());
            }
        }

        if !unique_topics.is_empty() {
            // Bounded so a slow / unreachable broker can't block the
            // caller indefinitely. `refresh_topics` itself has no
            // built-in deadline.
            match tokio::time::timeout(
                self.config.max_block,
                self.client.refresh_topics(&unique_topics),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(Error::RequestTimeout(format!(
                        "assign metadata refresh exceeded max_block={:?}",
                        self.config.max_block
                    )));
                }
            }
        }

        let (ack_tx, ack_rx) = oneshot::channel::<Result<()>>();
        // Unbounded send only fails if the fetcher exited (drop / close).
        self.assign_tx
            .send(AssignmentUpdate::Replace { tps, ack: ack_tx })
            .map_err(|_| Error::Protocol("consumer closed".into()))?;

        // Sender dropped without sending = fetcher panicked or shut
        // down between dispatch and ack; surface as closed so the
        // caller doesn't silently observe an empty assignment.
        match ack_rx.await {
            Ok(result) => result,
            Err(_) => Err(Error::Protocol("consumer closed".into())),
        }
    }

    /// Reposition the read cursor for a single assigned partition.
    /// Synchronous because `assign_tx` is unbounded and the fetcher
    /// applies the update on its next loop iteration. Best-effort: if
    /// `tp` isn't currently assigned the fetcher silently drops the
    /// update.
    pub fn seek(&self, tp: TopicPartition, offset: i64) -> Result<()> {
        // Unbounded send only fails if the fetcher exited (drop / close).
        self.assign_tx
            .send(AssignmentUpdate::Seek { tp, offset })
            .map_err(|_| Error::Protocol("consumer closed".into()))
    }

    /// Receive the next record from the fetcher. Returns `None` once
    /// the fetcher has exited and its in-flight buffer has drained.
    ///
    /// Escape hatch for callers who don't want to depend on
    /// [`futures_core::Stream`] / `StreamExt`.
    pub async fn recv(&mut self) -> Option<Result<ConsumerRecord>> {
        self.record_rx.recv().await
    }

    /// Signal the fetcher task to exit and await its completion.
    /// Mirrors [`Producer::close`].
    pub async fn close(mut self) -> Result<()> {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.fetcher_handle.take() {
            // JoinError only fires on panic / cancellation; either way
            // we've already signalled the task, so swallow it.
            let _ = handle.await;
        }
        Ok(())
    }
}

impl futures_core::Stream for Consumer {
    type Item = Result<ConsumerRecord>;

    // Yields `Ready(None)` when the inner channel is closed *and*
    // drained (the contract of `Receiver::poll_recv`), which happens
    // once the fetcher task exits and all `record_tx` clones are
    // dropped.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().record_rx.poll_recv(cx)
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // Consumer dropped without close(): signal shutdown so the
        // fetcher task exits, but don't await — Drop is sync. The
        // record channel will be closed once the fetcher drops its
        // `record_tx`, so any pending `recv` / `poll_next` resolves
        // to `None` after the in-flight buffer drains.
        if let Some(tx) = self.shutdown_tx.take() {
            tracing::warn!("consumer dropped without close()");
            let _ = tx.send(());
            // fetcher_handle deliberately leaked: Drop can't await it.
        }
    }
}
