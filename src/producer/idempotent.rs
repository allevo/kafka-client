//! Idempotent producer state (KIP-98).
//!
//! Holds the cluster-assigned `(producer_id, producer_epoch)` plus the
//! per-partition sequence space, in-flight queue, and recovery flags
//! the sender consults on every dispatch / response settle. The init
//! step (`InitProducerIdRequest`) is lazy: the first call to
//! [`IdempotentState::ensure_ready`] sends it, every subsequent call
//! is a fast cached read.
//!
//! The state machine mirrors Java's `TransactionManager`:
//!
//! - **Init.** `Uninitialized` → `Initializing` (one writer, others park
//!   on a `Notify`) → `Ready` or `Fatal`.
//! - **Per-partition.** `next_sequence` advances on `assign`,
//!   `last_acked_sequence` advances on `complete`. The encoded batch
//!   stays in the in-flight queue until the broker acks it so retries
//!   reuse the same bytes.
//! - **Recovery.** `OutOfOrderSequence` flags the partition for an
//!   epoch bump; `UnknownProducerId` resets the partition or escalates
//!   to a bump if predecessors are still in flight; `InvalidProducerEpoch`
//!   / `ProducerFenced` flips global state to `Fatal`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::init_producer_id_request::InitProducerIdRequest;
use kafka_protocol::messages::{ApiKey, InitProducerIdResponse, TopicName};
use tokio::sync::{Notify, oneshot};

use crate::client::{CallOptions, Client, NodeTarget, PartitionId};
use crate::error::{Error, Result};
use crate::producer::RecordMetadata;

/// Sentinel for "no record has been acked yet on this partition".
pub(crate) const NO_LAST_ACKED: i32 = -1;

/// Java's `DefaultRecordBatch.incrementSequence`: monotonic add that
/// wraps to 0 just past `i32::MAX` (sequences are unsigned-wrapped on
/// the wire, so the broker performs the same arithmetic).
pub(crate) fn increment_sequence(seq: i32, increment: i32) -> i32 {
    debug_assert!(seq >= 0, "sequences are non-negative");
    debug_assert!(increment >= 0, "increment must be non-negative");
    if seq > i32::MAX - increment {
        increment - (i32::MAX - seq) - 1
    } else {
        seq + increment
    }
}

/// Exponential retry backoff for a per-partition in-flight batch:
/// `base * 2^(attempt - 1)` with ±20 % jitter, capped at `max` after
/// jitter (matches Java's `ExponentialBackoff` —
/// `CommonClientConfigs.RETRY_BACKOFF_EXP_BASE = 2`,
/// `RETRY_BACKOFF_JITTER = 0.2` — and librdkafka's
/// `RD_KAFKA_RETRY_JITTER_PERCENT = 20`). `attempt == 0` is "fresh, no
/// retries yet" and returns `Duration::ZERO` — the very first send
/// must not be delayed.
pub(crate) fn retry_delay(attempt: u32, base: Duration, max: Duration) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    // Cap the shift at 31 so `1u128 << shift` cannot panic. Anything
    // past that saturates to `max` anyway.
    let shift = attempt.saturating_sub(1).min(31);
    let multiplier: u128 = 1u128 << shift;
    let nominal = base.as_nanos().saturating_mul(multiplier);
    // Symmetric ±20 % jitter. fastrand is already a dependency (used
    // by `client::retry::next_backoff` for the reconnect backoff).
    let jitter_span = (nominal / 5) as i128;
    let jitter = if jitter_span > 0 {
        fastrand::i128(-jitter_span..=jitter_span)
    } else {
        0
    };
    let jittered = (nominal as i128 + jitter).max(0) as u128;
    // Cap *after* jitter so an unlucky high-side jitter draw cannot
    // push the wait past the configured maximum — Java's
    // `ExponentialBackoff.backoff` applies the same ordering.
    let capped = jittered.min(max.as_nanos());
    let nanos_u64 = capped.min(u64::MAX as u128) as u64;
    Duration::from_nanos(nanos_u64)
}

/// Result of [`IdempotentState::assign`]: the producer-state to stamp on
/// the next `freeze()`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Assigned {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

/// Lightweight snapshot of one partition's head in-flight batch for
/// the dispatch loop to inspect without holding the state mutex.
#[derive(Clone)]
pub(crate) struct InflightHeadSnapshot {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub base_sequence: i32,
    pub encoded: Bytes,
    pub first_send_at: Instant,
    pub attempt: u32,
    /// Earliest `Instant` at which this batch may be re-dispatched.
    /// Equals `Instant::now()` for a freshly registered batch; grows
    /// exponentially after each retry-bump so a misbehaving broker
    /// can't be hammered at linger cadence (Java `retry.backoff.ms`).
    pub next_attempt_at: Instant,
}

/// Why an `assign` could not return a fresh sequence right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AssignError {
    /// The producer-id has not been acquired yet. The sender awaits
    /// [`IdempotentState::ensure_ready`] before retrying.
    NotReady,
    /// Idempotence is in a terminal failure state.
    Fatal,
    /// The partition is awaiting a per-partition epoch bump; the
    /// sender must re-buffer the batch and try on the next tick.
    PartitionBlocked,
}

/// How the epoch-bump pass must treat a flagged partition (T3a).
///
/// The distinction guards the exactly-once invariant once multi-head
/// dispatch puts more than the head batch on the wire per partition:
/// the producer must never transmit a record under a fresh
/// `(producer_id, producer_epoch)` while a prior transmission of that
/// record under the old epoch could still be accepted by the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BumpMode {
    /// The broker explicitly rejected the partition's head with a
    /// sequence-coherent error (`OutOfOrderSequence`, `UnknownProducerId`
    /// with predecessors, `MessageTooLarge`, a fatal produce code). A
    /// rejected head implies every higher-sequence successor is also
    /// rejected, so *nothing* currently in the in-flight queue can have
    /// been accepted — the bump pass re-stamps the whole queue from
    /// `base_sequence = 0` immediately (Java's `partitionsToRewriteSequences`).
    Eager,
    /// The head was given up on *without* a broker rejection: a
    /// `delivery_timeout` / retries-exceeded pop, a metadata loss, or a
    /// sibling batch's drain-time `freeze` failure. A successor — or the
    /// timed-out head itself — may already have been accepted by the
    /// broker (its response merely lost), so the bump pass must NOT
    /// re-stamp: it acquires a fresh `(pid, epoch)`, parks it as the
    /// partition's `pending_epoch_reset`, and lets the old-epoch
    /// in-flight queue drain under the old epoch before applying the
    /// reset. Mirrors librdkafka's `RD_KAFKA_IDEMP_STATE_DRAIN_BUMP` and
    /// Java's `hasStaleProducerIdAndEpoch` + `maybeUpdateProducerIdAndEpoch`.
    Deferred,
}

/// Merge a bump request into the pending set. `Eager` always wins over
/// `Deferred`: an eager request means the broker provably rejected the
/// head, so the whole queue is provably unsent and an immediate
/// re-stamp is sound even if a deferred trigger also fired for the same
/// partition.
fn upsert_pending_bump(
    map: &mut HashMap<(TopicName, PartitionId), BumpMode>,
    key: (TopicName, PartitionId),
    mode: BumpMode,
) {
    match map.entry(key) {
        std::collections::hash_map::Entry::Occupied(mut e) => {
            if matches!(mode, BumpMode::Eager) {
                e.insert(BumpMode::Eager);
            }
        }
        std::collections::hash_map::Entry::Vacant(e) => {
            e.insert(mode);
        }
    }
}

/// Apply a parked deferred epoch reset the moment a partition's
/// in-flight queue empties: every old-epoch batch has now been acked or
/// terminally failed, so swapping in the fresh `(pid, epoch)` and
/// rewinding `next_sequence` to 0 can no longer duplicate an
/// accepted-but-unconfirmed record. Mirrors Java's
/// `maybeUpdateProducerIdAndEpoch` (`!hasInflightBatches` guard).
fn maybe_apply_pending_reset(part: &mut PartitionState) {
    if part.in_flight.is_empty()
        && let Some((new_pid, new_epoch)) = part.pending_epoch_reset.take()
    {
        part.producer_id = new_pid;
        part.producer_epoch = new_epoch;
        part.next_sequence = 0;
        part.last_acked_sequence = NO_LAST_ACKED;
    }
}

#[derive(Debug)]
enum InitState {
    Uninitialized,
    Initializing,
    Ready { producer_id: i64, producer_epoch: i16 },
    Fatal(String),
}

/// Per-partition sequence-state book-keeping.
///
/// `producer_id` / `producer_epoch` may transiently diverge from the
/// global ones after a partition-local bump (the Java client allows
/// this; see `TransactionManager.bumpIdempotentEpochAndResetIdIfNeeded`).
/// Initially they track the global Ready state.
struct PartitionState {
    producer_id: i64,
    producer_epoch: i16,
    next_sequence: i32,
    last_acked_sequence: i32,
    in_flight: VecDeque<InFlightBatch>,
    /// When `Some((new_pid, new_epoch))`, a [`BumpMode::Deferred`] epoch
    /// bump has been acquired from the broker but not yet applied: the
    /// partition is draining its old-epoch in-flight queue under the
    /// *old* `(producer_id, producer_epoch)`. While this is `Some`,
    /// `assign` is blocked and the accumulator leaves the partition's
    /// buffered batches un-drained (the `shouldStopDrainBatchesForPartition`
    /// analog) — a fresh seq-0 batch under the new epoch must not be
    /// written ahead of unresolved old-epoch batches. The moment
    /// `in_flight` empties, [`maybe_apply_pending_reset`] swaps in the
    /// new `(pid, epoch)`, rewinds `next_sequence` to 0, and clears this.
    pending_epoch_reset: Option<(i64, i16)>,
}

/// One in-flight produce batch held until the broker acks (or the
/// dispatch loop retries it). `encoded` is the wire-format bytes
/// stamped with `(producer_id, producer_epoch, base_sequence)` so a
/// retransmission ships the exact same record-batch the broker already
/// dedupes against. An epoch-bump rewrite (slice C1) mutates a
/// `BytesMut` clone of `encoded` in place via
/// [`crate::producer::batch_rewrite::rewrite_producer_state`] — no
/// decoded record copy is retained, mirroring Java's
/// `MemoryRecordsBuilder.reopenAndRewriteProducerState` design.
pub(crate) struct InFlightBatch {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub base_sequence: i32,
    pub record_count: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub encoded: Bytes,
    pub waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    pub first_send_at: Instant,
    pub attempt: u32,
    /// Earliest `Instant` at which the dispatch loop may re-send this
    /// batch. Set to `now` on registration; grows on every retry-bump
    /// via `retry_backoff * 2^attempt` capped at `retry_backoff_max`
    /// (Java `retry.backoff.ms` / `retry.backoff.max.ms`).
    pub next_attempt_at: Instant,
}

struct Inner {
    init: InitState,
    partitions: HashMap<(TopicName, PartitionId), PartitionState>,
    /// Partitions awaiting an epoch bump on the next dispatch tick, each
    /// tagged with how the bump pass must treat it ([`BumpMode`]). A
    /// partition already mid-deferred-reset (`pending_epoch_reset` set)
    /// is never (re-)added — it has a fresh epoch acquired already.
    pending_bumps: HashMap<(TopicName, PartitionId), BumpMode>,
}

pub(crate) struct IdempotentState {
    inner: Mutex<Inner>,
    /// Wakes every parked `ensure_ready` caller when init transitions
    /// out of `Initializing`.
    init_done: Notify,
    /// Sum of `encoded.len()` across every batch currently sitting in a
    /// per-partition in-flight queue. Maintained on every queue mutation
    /// so `Accumulator::reserve` can charge in-flight bytes against
    /// `buffer_memory` (T2c / N8): a batch that has drained out of the
    /// accumulator is no longer counted in `bytes_in_use`, so without
    /// this a stalled broker would hoard arbitrary memory through
    /// retried in-flight batches even with a cap configured.
    total_inflight_bytes: AtomicUsize,
}

impl IdempotentState {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                init: InitState::Uninitialized,
                partitions: HashMap::new(),
                pending_bumps: HashMap::new(),
            }),
            init_done: Notify::new(),
            total_inflight_bytes: AtomicUsize::new(0),
        }
    }

    /// Total bytes of encoded record-batches sitting in every
    /// per-partition in-flight queue. Read by `Accumulator::reserve` to
    /// charge in-flight batches against `buffer_memory` (T2c / N8).
    pub(crate) fn inflight_bytes(&self) -> usize {
        self.total_inflight_bytes.load(Ordering::Relaxed)
    }

    /// A batch entered an in-flight queue — add its encoded size to the
    /// accounting. Paired 1:1 with [`Self::release_inflight_bytes`]:
    /// every batch is counted on insertion and uncounted exactly once
    /// on removal.
    fn track_inflight_bytes(&self, n: usize) {
        self.total_inflight_bytes.fetch_add(n, Ordering::Relaxed);
    }

    /// A batch left an in-flight queue — subtract its encoded size. The
    /// `saturating_sub`-via-CAS keeps the counter sane even if the
    /// accounting were ever miscounted; in correct use it never
    /// saturates (every batch is uncounted exactly once), mirroring the
    /// defensive stance of the accumulator's `bytes_in_use` book-keeping.
    fn release_inflight_bytes(&self, n: usize) {
        let mut cur = self.total_inflight_bytes.load(Ordering::Relaxed);
        loop {
            match self.total_inflight_bytes.compare_exchange_weak(
                cur,
                cur.saturating_sub(n),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Block until init reaches `Ready` (or `Fatal`). The first caller
    /// performs the `InitProducerIdRequest`; concurrent callers park on
    /// `init_done` and wake when the writer finishes.
    pub(crate) async fn ensure_ready(&self, client: &Client) -> Result<(i64, i16)> {
        loop {
            // Listen for completion BEFORE re-checking state: tokio's
            // Notify is not sticky for new listeners, so subscribing
            // first eliminates the lost-wakeup race when the writer
            // finishes between our state check and our await.
            let notified = self.init_done.notified();
            tokio::pin!(notified);

            let do_init = {
                let mut guard = self.inner.lock().unwrap();
                match &guard.init {
                    InitState::Ready {
                        producer_id,
                        producer_epoch,
                    } => return Ok((*producer_id, *producer_epoch)),
                    InitState::Fatal(s) => return Err(Error::IdempotenceFenced(s.clone())),
                    InitState::Initializing => false,
                    InitState::Uninitialized => {
                        guard.init = InitState::Initializing;
                        true
                    }
                }
            };

            if do_init {
                let outcome = send_init(client, -1, -1).await;
                let mut guard = self.inner.lock().unwrap();
                match outcome {
                    Ok((pid, epoch)) => {
                        tracing::info!(producer_id = pid, producer_epoch = epoch, "idempotent producer initialized");
                        guard.init = InitState::Ready {
                            producer_id: pid,
                            producer_epoch: epoch,
                        };
                        self.init_done.notify_waiters();
                        return Ok((pid, epoch));
                    }
                    Err(e) => {
                        let msg = format!("InitProducerId failed: {e}");
                        guard.init = InitState::Fatal(msg.clone());
                        self.init_done.notify_waiters();
                        return Err(Error::IdempotenceFenced(msg));
                    }
                }
            }

            // Park on init_done and recheck when the writer finishes.
            notified.await;
        }
    }

    /// Reserve a `base_sequence` for `record_count` records on `(topic,
    /// partition)`. Advances `next_sequence` so the next `assign` runs
    /// from `base_sequence + record_count`.
    pub(crate) fn assign(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        record_count: usize,
    ) -> std::result::Result<Assigned, AssignError> {
        let mut guard = self.inner.lock().unwrap();

        let (global_pid, global_epoch) = match &guard.init {
            InitState::Ready {
                producer_id,
                producer_epoch,
            } => (*producer_id, *producer_epoch),
            InitState::Fatal(_) => return Err(AssignError::Fatal),
            InitState::Uninitialized | InitState::Initializing => return Err(AssignError::NotReady),
        };

        let key = (topic.clone(), partition);
        // Blocked while a bump is pending (either flavour) or a deferred
        // reset is mid-flight — the drain skips these partitions, so in
        // the normal flow `assign` is never even called for one; the
        // check stays as a fail-safe (and backs the `assign`-level unit
        // tests).
        if guard.pending_bumps.contains_key(&key) {
            return Err(AssignError::PartitionBlocked);
        }
        if guard
            .partitions
            .get(&key)
            .is_some_and(|p| p.pending_epoch_reset.is_some())
        {
            return Err(AssignError::PartitionBlocked);
        }

        let part = guard.partitions.entry(key).or_insert_with(|| PartitionState {
            producer_id: global_pid,
            producer_epoch: global_epoch,
            next_sequence: 0,
            last_acked_sequence: NO_LAST_ACKED,
            in_flight: VecDeque::new(),
            pending_epoch_reset: None,
        });

        let base_sequence = part.next_sequence;
        // Cap record_count at i32::MAX so the `as i32` below cannot
        // wrap into a negative increment — the encoder rejects batches
        // larger than i32 anyway, so this only changes the failure mode
        // (saturating cap vs. silent corruption).
        let increment = i32::try_from(record_count).unwrap_or(i32::MAX);
        part.next_sequence = increment_sequence(part.next_sequence, increment);

        Ok(Assigned {
            producer_id: part.producer_id,
            producer_epoch: part.producer_epoch,
            base_sequence,
        })
    }

    /// Register an in-flight batch that the sender is about to
    /// dispatch. Held in the partition queue until the broker response
    /// settles it, so retries can re-dispatch the same encoded bytes.
    pub(crate) fn register_inflight(&self, batch: InFlightBatch) {
        let key = (batch.topic.clone(), batch.partition);
        let batch_bytes = batch.encoded.len();
        let mut guard = self.inner.lock().unwrap();
        let Some(part) = guard.partitions.get_mut(&key) else {
            // assign() must precede register_inflight, so the partition
            // entry exists. Defensive log + drop on impossible state.
            tracing::error!(
                topic = batch.topic.as_str(),
                partition = batch.partition.0,
                "register_inflight called without a prior assign — dropping batch",
            );
            return;
        };
        part.in_flight.push_back(batch);
        // Count the batch against `buffer_memory` only after it is
        // actually queued — the early return above drops the batch, so
        // its bytes must not be tracked.
        self.track_inflight_bytes(batch_bytes);
    }

    /// Settle a successful response: drop the head batch (assumed to
    /// match `base_sequence`) and advance `last_acked_sequence`.
    /// Returns the in-flight entry so the sender can resolve waiters.
    pub(crate) fn complete(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        base_sequence: i32,
    ) -> Option<InFlightBatch> {
        let mut guard = self.inner.lock().unwrap();
        let part = guard.partitions.get_mut(&(topic.clone(), partition))?;
        // Match by base_sequence so an out-of-order response can't
        // dequeue the wrong batch.
        let head = part.in_flight.front()?;
        if head.base_sequence != base_sequence {
            tracing::warn!(
                topic = topic.as_str(),
                partition = partition.0,
                expected = head.base_sequence,
                got = base_sequence,
                "complete: base_sequence mismatch — keeping head",
            );
            return None;
        }
        let batch = part.in_flight.pop_front()?;
        self.release_inflight_bytes(batch.encoded.len());
        // last_acked is the *last record's* sequence in this batch;
        // record_count is at least 1 by construction.
        let last_in_batch =
            increment_sequence(batch.base_sequence, batch.record_count.saturating_sub(1));
        part.last_acked_sequence = last_in_batch;
        // This pop may have drained the last old-epoch batch on a
        // partition with a parked deferred epoch reset — apply it now
        // that nothing accepted-but-unconfirmed remains (T3a).
        maybe_apply_pending_reset(part);
        Some(batch)
    }

    /// Pop the head in-flight batch on `(topic, partition)` regardless
    /// of error code, so the sender can decide what to do with it
    /// (resolve waiters with an error, requeue, etc).
    pub(crate) fn pop_inflight_head(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<InFlightBatch> {
        let mut guard = self.inner.lock().unwrap();
        let part = guard.partitions.get_mut(&(topic.clone(), partition))?;
        let popped = part.in_flight.pop_front();
        if let Some(batch) = &popped {
            self.release_inflight_bytes(batch.encoded.len());
        }
        popped
    }

    /// Fail the head in-flight batch on `(topic, partition)`: pop it,
    /// flag the partition for an epoch bump with the given [`BumpMode`]
    /// (atomically with the pop, so no in-between `assign` can hand out
    /// a poisoned `base_sequence`), and notify the popped batch's
    /// waiters via `make_err`.
    ///
    /// `assign()` advances `next_sequence` eagerly, so any non-success
    /// pop leaves the partition's sequence space ahead of what the
    /// broker has actually accepted — the bump flag is what closes that
    /// gap before the next batch ships. Mirrors Java's
    /// `TransactionManager.handleFailedBatch` (which calls
    /// `requestIdempotentEpochBumpForPartition` for non-transactional
    /// idempotent producers; see `TransactionManager.java:819-841`).
    ///
    /// `mode` is [`BumpMode::Eager`] when the broker provably rejected
    /// the head (a fatal produce code) and [`BumpMode::Deferred`] when
    /// it was given up on without a rejection (`delivery_timeout`,
    /// retries-exceeded, metadata loss) — see [`BumpMode`]. A partition
    /// already mid-deferred-reset is left alone: the pop just advances
    /// its drain toward the parked reset.
    pub(crate) fn fail_inflight_head(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        mode: BumpMode,
        make_err: impl Fn() -> Error,
    ) {
        let popped = {
            let mut guard = self.inner.lock().unwrap();
            let key = (topic.clone(), partition);
            let popped = guard
                .partitions
                .get_mut(&key)
                .and_then(|p| p.in_flight.pop_front());
            // Only act if we owned a head to pop. An empty queue means
            // there's nothing to recover — flagging would trigger a
            // no-op bump pass.
            if let Some(batch) = &popped {
                self.release_inflight_bytes(batch.encoded.len());
                let already_resetting = guard
                    .partitions
                    .get(&key)
                    .is_some_and(|p| p.pending_epoch_reset.is_some());
                if already_resetting {
                    // A fresh epoch is already acquired and parked; the
                    // pop above may have drained the last old-epoch
                    // batch — apply the reset now if so.
                    if let Some(p) = guard.partitions.get_mut(&key) {
                        maybe_apply_pending_reset(p);
                    }
                } else {
                    upsert_pending_bump(&mut guard.pending_bumps, key, mode);
                }
            }
            popped
        };
        if let Some(batch) = popped {
            for tx in batch.waiters {
                let _ = tx.send(Err(make_err()));
            }
        }
    }

    /// Push a batch back onto the front of the partition's in-flight
    /// queue. Used by the `MessageTooLarge` split-and-requeue recovery
    /// to re-insert both halves of an oversized batch ahead of its
    /// successors without losing per-partition FIFO order.
    pub(crate) fn requeue_inflight_front(&self, batch: InFlightBatch) {
        let key = (batch.topic.clone(), batch.partition);
        let batch_bytes = batch.encoded.len();
        let mut guard = self.inner.lock().unwrap();
        let part = guard.partitions.entry(key).or_insert_with(|| PartitionState {
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            // Conservative: if the partition entry vanished while we
            // had the in-flight batch, restore next_sequence so it
            // covers everything we know was sent.
            next_sequence: increment_sequence(batch.base_sequence, batch.record_count),
            last_acked_sequence: NO_LAST_ACKED,
            in_flight: VecDeque::new(),
            pending_epoch_reset: None,
        });
        part.in_flight.push_front(batch);
        self.track_inflight_bytes(batch_bytes);
    }

    /// Drain every in-flight batch on `(topic, partition)` so the
    /// sender can release waiters with a fatal/skip error.
    pub(crate) fn drain_partition_inflight(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Vec<InFlightBatch> {
        let mut guard = self.inner.lock().unwrap();
        let Some(part) = guard.partitions.get_mut(&(topic.clone(), partition)) else {
            return Vec::new();
        };
        let drained: Vec<InFlightBatch> = part.in_flight.drain(..).collect();
        let freed: usize = drained.iter().map(|b| b.encoded.len()).sum();
        self.release_inflight_bytes(freed);
        drained
    }

    /// Reset a partition's sequence space (`next_sequence = 0`,
    /// `last_acked = NO_LAST_ACKED`). Used on `UnknownProducerId` when
    /// the head in-flight batch had no predecessors — the broker has
    /// forgotten about us, but our sequences are coherent.
    pub(crate) fn reset_partition(&self, topic: &TopicName, partition: PartitionId) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(part) = guard.partitions.get_mut(&(topic.clone(), partition)) {
            part.next_sequence = 0;
            part.last_acked_sequence = NO_LAST_ACKED;
        }
    }

    /// Mark `(topic, partition)` as needing an epoch bump on the next
    /// dispatch tick, tagged with `mode` ([`BumpMode`]). The sender
    /// drains pending bumps via [`Self::take_pending_bumps`].
    ///
    /// A no-op when the partition is already mid-deferred-reset: a fresh
    /// `(pid, epoch)` is already parked, and its old-epoch in-flight
    /// batches keep retransmitting under the old epoch until the queue
    /// drains. `Eager` upgrades a previously-`Deferred` request for the
    /// same partition (see [`upsert_pending_bump`]).
    pub(crate) fn request_partition_bump(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        mode: BumpMode,
    ) {
        let mut guard = self.inner.lock().unwrap();
        let key = (topic.clone(), partition);
        if guard
            .partitions
            .get(&key)
            .is_some_and(|p| p.pending_epoch_reset.is_some())
        {
            return;
        }
        upsert_pending_bump(&mut guard.pending_bumps, key, mode);
    }

    /// Take and clear the set of partitions awaiting an epoch bump, each
    /// tagged with the [`BumpMode`] the bump pass must apply.
    pub(crate) fn take_pending_bumps(&self) -> Vec<((TopicName, PartitionId), BumpMode)> {
        let mut guard = self.inner.lock().unwrap();
        guard.pending_bumps.drain().collect()
    }

    /// Park a [`BumpMode::Deferred`] epoch bump: the broker has handed
    /// back a fresh `(new_pid, new_epoch)`, but the partition's
    /// old-epoch in-flight batches must keep retransmitting under the
    /// *old* `(pid, epoch)` until the queue drains — only then is it
    /// safe to swap in the new state and rewind `next_sequence` to 0
    /// (T3a's exactly-once guard). If the queue is already empty (every
    /// old-epoch batch acked or terminally failed while the bump request
    /// was waiting for the pass), the reset is applied immediately.
    /// Clears any stale `pending_bumps` entry for the partition.
    pub(crate) fn begin_deferred_epoch_reset(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        new_pid: i64,
        new_epoch: i16,
    ) {
        let mut guard = self.inner.lock().unwrap();
        let key = (topic.clone(), partition);
        guard.pending_bumps.remove(&key);
        let Some(part) = guard.partitions.get_mut(&key) else {
            // Partition entry vanished — nothing in flight to drain, and
            // a later `assign` will start it fresh from the global state.
            return;
        };
        part.pending_epoch_reset = Some((new_pid, new_epoch));
        // The old-epoch queue may already have drained while the bump
        // request was waiting for this pass — apply the reset now if so.
        maybe_apply_pending_reset(part);
    }

    /// Snapshot every partition the accumulator must NOT drain new
    /// batches for: one with a pending epoch bump (the next tick's bump
    /// pass will re-stamp / re-acquire its state) or a parked deferred
    /// reset (its old-epoch in-flight queue is still draining). The
    /// `shouldStopDrainBatchesForPartition` analog — see
    /// [`PartitionState::pending_epoch_reset`].
    pub(crate) fn drain_blocked_partitions(&self) -> HashSet<(TopicName, PartitionId)> {
        let guard = self.inner.lock().unwrap();
        let mut set: HashSet<(TopicName, PartitionId)> =
            guard.pending_bumps.keys().cloned().collect();
        for (key, part) in &guard.partitions {
            if part.pending_epoch_reset.is_some() {
                set.insert(key.clone());
            }
        }
        set
    }

    /// True iff `(topic, partition)` is flagged for a [`BumpMode::Eager`]
    /// bump. The multi-head dispatch loop skips such partitions: their
    /// in-flight queue is about to be re-stamped wholesale by the next
    /// tick's bump pass, so re-dispatching the (rejected) head this tick
    /// would only draw another rejection. A [`BumpMode::Deferred`]
    /// partition is *not* skipped — its old-epoch batches must keep
    /// retransmitting to drain the queue toward the parked reset.
    pub(crate) fn is_eager_bump_pending(&self, topic: &TopicName, partition: PartitionId) -> bool {
        matches!(
            self.inner
                .lock()
                .unwrap()
                .pending_bumps
                .get(&(topic.clone(), partition)),
            Some(BumpMode::Eager),
        )
    }

    /// Drop every in-progress recovery — pending bumps and parked
    /// deferred resets alike. Called on the fatal-shutdown path
    /// (`drain_and_fail_everything`) so the subsequent `drain_all` is
    /// not blocked from flushing a partition's buffered waiters: once
    /// the producer is fatal, sequence-space recovery is moot.
    pub(crate) fn abort_all_recovery(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.pending_bumps.clear();
        for part in guard.partitions.values_mut() {
            part.pending_epoch_reset = None;
        }
    }

    /// Are there any partitions awaiting a bump? Cheap probe so the
    /// sender can short-circuit the bump pass on the common path.
    pub(crate) fn has_pending_bumps(&self) -> bool {
        let guard = self.inner.lock().unwrap();
        !guard.pending_bumps.is_empty()
    }

    /// Transition to terminal `Fatal` state. Every subsequent send
    /// fails with `Error::IdempotenceFenced(reason)`. Idempotent — a
    /// second call replaces the reason.
    pub(crate) fn mark_fatal(&self, reason: String) {
        let mut guard = self.inner.lock().unwrap();
        guard.init = InitState::Fatal(reason);
        self.init_done.notify_waiters();
    }

    /// True iff state has transitioned to `Fatal`.
    pub(crate) fn is_fatal(&self) -> bool {
        matches!(self.inner.lock().unwrap().init, InitState::Fatal(_))
    }

    /// Borrow the `Fatal` reason string (for resolving live waiters
    /// after a fatal transition).
    pub(crate) fn fatal_reason(&self) -> Option<String> {
        match &self.inner.lock().unwrap().init {
            InitState::Fatal(s) => Some(s.clone()),
            _ => None,
        }
    }

    /// Borrow the current `(producer_id, producer_epoch)` of a
    /// partition. Returns `None` when the partition has never been
    /// assigned (so there's nothing to bump). The bump pass reads this
    /// to pass the partition's current state into `request_epoch_bump`
    /// — Kafka's `InitProducerIdRequest` v3+ accepts the caller's
    /// current `(pid, epoch)` so a transactional coordinator can issue
    /// a fresh epoch tied to the same producer-id.
    pub(crate) fn partition_state(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<(i64, i16)> {
        let guard = self.inner.lock().unwrap();
        guard
            .partitions
            .get(&(topic.clone(), partition))
            .map(|p| (p.producer_id, p.producer_epoch))
    }

    /// Apply a successful per-partition epoch-bump response. Updates
    /// the partition's `(producer_id, producer_epoch)`, clears the
    /// pending-bump flag, and resets `next_sequence = 0` /
    /// `last_acked_sequence = NO_LAST_ACKED`. Returns the in-flight
    /// queue so the sender can rewrite + re-encode each batch with
    /// the new state.
    pub(crate) fn apply_bumped_state(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        new_pid: i64,
        new_epoch: i16,
    ) -> Vec<InFlightBatch> {
        let mut guard = self.inner.lock().unwrap();
        guard.pending_bumps.remove(&(topic.clone(), partition));
        let Some(part) = guard.partitions.get_mut(&(topic.clone(), partition)) else {
            return Vec::new();
        };
        part.producer_id = new_pid;
        part.producer_epoch = new_epoch;
        part.next_sequence = 0;
        part.last_acked_sequence = NO_LAST_ACKED;
        // An eager bump fully re-stamps the queue here; a partition can
        // never be both eager-flagged and mid-deferred-reset (a deferred
        // reset no-ops any later bump request), but clear it defensively
        // so a stale `Some` can't wedge `assign` after the rewrite.
        part.pending_epoch_reset = None;
        let drained: Vec<InFlightBatch> = part.in_flight.drain(..).collect();
        let freed: usize = drained.iter().map(|b| b.encoded.len()).sum();
        self.release_inflight_bytes(freed);
        drained
    }

    /// Snapshot every (topic, partition) the state has seen, so a
    /// global drain helper can iterate without holding the inner
    /// mutex.
    pub(crate) fn known_partitions(&self) -> Vec<(TopicName, PartitionId)> {
        self.inner
            .lock()
            .unwrap()
            .partitions
            .keys()
            .cloned()
            .collect()
    }

    /// Snapshot every in-flight head batch's identifying fields and
    /// encoded bytes (cheap clone). Used by the dispatch loop to build
    /// the next round of `ProduceRequest`s.
    pub(crate) fn snapshot_inflight(&self) -> Vec<InflightHeadSnapshot> {
        let guard = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for ((topic, partition), part) in guard.partitions.iter() {
            if let Some(head) = part.in_flight.front() {
                out.push(InflightHeadSnapshot {
                    topic: topic.clone(),
                    partition: *partition,
                    base_sequence: head.base_sequence,
                    encoded: head.encoded.clone(),
                    first_send_at: head.first_send_at,
                    attempt: head.attempt,
                    next_attempt_at: head.next_attempt_at,
                });
            }
        }
        out
    }

    /// Bump the head batch's `attempt` counter on every `(topic,
    /// partition)` whose key is in `keys` and arm `next_attempt_at` so
    /// the dispatch loop skips the head until the backoff elapses.
    /// Used when a transport-level failure took down the produce
    /// request as a whole.
    pub(crate) fn bump_attempts(
        &self,
        keys: &[(TopicName, PartitionId, i32)],
        retry_backoff: Duration,
        retry_backoff_max: Duration,
    ) {
        let now = Instant::now();
        let mut guard = self.inner.lock().unwrap();
        for (topic, partition, _) in keys {
            if let Some(part) = guard.partitions.get_mut(&(topic.clone(), *partition))
                && let Some(head) = part.in_flight.front_mut()
            {
                head.attempt = head.attempt.saturating_add(1);
                head.next_attempt_at =
                    now + retry_delay(head.attempt, retry_backoff, retry_backoff_max);
            }
        }
    }

    /// Bump the head batch's attempt on a single partition and arm
    /// `next_attempt_at` with the exponential backoff.
    pub(crate) fn bump_attempt_one(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        retry_backoff: Duration,
        retry_backoff_max: Duration,
    ) {
        let now = Instant::now();
        let mut guard = self.inner.lock().unwrap();
        if let Some(part) = guard.partitions.get_mut(&(topic.clone(), partition))
            && let Some(head) = part.in_flight.front_mut()
        {
            head.attempt = head.attempt.saturating_add(1);
            head.next_attempt_at =
                now + retry_delay(head.attempt, retry_backoff, retry_backoff_max);
        }
    }

    /// Number of in-flight batches *before* the one whose
    /// `base_sequence` matches the argument. Used to decide whether
    /// `UnknownProducerId` can be recovered by a partition reset
    /// (`Some(0)` — match at head) or needs a full epoch bump
    /// (`Some(n>0)` — predecessors still in flight). `None` means no
    /// in-flight batch carries this `base_sequence` — the response is
    /// stale or out-of-order and the caller should leave the queue
    /// untouched, otherwise it would pop an unrelated head.
    pub(crate) fn partition_inflight_predecessor_count(
        &self,
        topic: &TopicName,
        partition: PartitionId,
        base_sequence: i32,
    ) -> Option<usize> {
        let guard = self.inner.lock().unwrap();
        let part = guard.partitions.get(&(topic.clone(), partition))?;
        part.in_flight
            .iter()
            .position(|b| b.base_sequence == base_sequence)
    }

    /// Test-only: force `init` to `Ready(pid, epoch)` without going
    /// through `ensure_ready`. Used by sibling-module tests that exercise
    /// post-init code paths in isolation from a real broker.
    #[cfg(test)]
    pub(crate) fn force_ready_for_test(&self, producer_id: i64, producer_epoch: i16) {
        let mut guard = self.inner.lock().unwrap();
        guard.init = InitState::Ready {
            producer_id,
            producer_epoch,
        };
    }

    /// Test-only: read a partition's `next_sequence` without exposing
    /// internal state through the public API.
    #[cfg(test)]
    pub(crate) fn partition_next_sequence_for_test(
        &self,
        topic: &TopicName,
        partition: PartitionId,
    ) -> Option<i32> {
        let guard = self.inner.lock().unwrap();
        guard
            .partitions
            .get(&(topic.clone(), partition))
            .map(|p| p.next_sequence)
    }

    /// Push a freshly re-encoded batch onto the partition's in-flight
    /// queue and advance `next_sequence` past it. Used after a bump
    /// rewrite has produced new encoded bytes.
    pub(crate) fn install_rewritten_inflight(&self, batch: InFlightBatch) {
        let key = (batch.topic.clone(), batch.partition);
        let batch_bytes = batch.encoded.len();
        let mut guard = self.inner.lock().unwrap();
        let part = guard.partitions.entry(key).or_insert_with(|| PartitionState {
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            next_sequence: 0,
            last_acked_sequence: NO_LAST_ACKED,
            in_flight: VecDeque::new(),
            pending_epoch_reset: None,
        });
        part.next_sequence =
            increment_sequence(batch.base_sequence, batch.record_count);
        part.in_flight.push_back(batch);
        self.track_inflight_bytes(batch_bytes);
    }
}

/// Send a non-transactional `InitProducerIdRequest`. `transactional_id =
/// None` puts the broker into the idempotent code path (KIP-98 only;
/// txn coordination — KIP-360 — is out of scope for this slice).
///
/// Retries internally on retryable broker error codes
/// (`CoordinatorLoadInProgress`, `NotCoordinator`, …) because those
/// codes live in the response *payload*, not the transport layer —
/// `Client::send`'s retry loop classifies on `Result<_, Error>` and
/// does not see them.
async fn send_init(client: &Client, current_pid: i64, current_epoch: i16) -> Result<(i64, i16)> {
    let mut backoff = Duration::from_millis(50);
    let max_backoff = Duration::from_secs(2);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let request = InitProducerIdRequest::default()
            .with_transactional_id(None)
            // Java uses Integer.MAX_VALUE for non-txn idempotent producers.
            // The broker only honours this for transactional ids; non-txn
            // sends are unaffected by the value.
            .with_transaction_timeout_ms(i32::MAX)
            .with_producer_id(current_pid.into())
            .with_producer_epoch(current_epoch);

        let response: InitProducerIdResponse = client
            .send(
                NodeTarget::AnyBroker,
                ApiKey::InitProducerId,
                // v4: matches the v9 ProduceRequest we ship; supports
                // producer_id / producer_epoch fields needed for
                // per-partition epoch bumps.
                4,
                request,
                // Lean on the client-level retry loop for transport-level
                // failures; we handle response-level error codes below.
                &CallOptions::new(),
            )
            .await?;

        match ResponseError::try_from_code(response.error_code) {
            None => return Ok((response.producer_id.0, response.producer_epoch)),
            Some(err) => {
                let classified = (Error::Broker { error: err }).classify();
                if !matches!(
                    classified,
                    crate::error::RetryAction::Retry | crate::error::RetryAction::RefreshMetadata
                ) {
                    return Err(Error::Broker { error: err });
                }
                if tokio::time::Instant::now() + backoff > deadline {
                    return Err(Error::RequestTimeout(format!(
                        "InitProducerId still {err:?} after retry budget"
                    )));
                }
                tracing::warn!(
                    error = ?err,
                    backoff_ms = backoff.as_millis() as u64,
                    "InitProducerId returned retryable error; backing off",
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

/// Public-facing alias of [`send_init`] for the per-partition
/// epoch-bump path. Carries the partition's current
/// `(producer_id, producer_epoch)` so the broker can issue a fresh
/// epoch tied to the same producer-id (api version ≥ 3).
///
/// NB: For non-transactional idempotent producers, Java
/// (`TransactionManager.bumpIdempotentProducerEpoch`) and librdkafka
/// (`rd_kafka_pid_bump` in `rdkafka_idempotence.c`) bump the epoch
/// **locally** without contacting the broker — only the PID reset
/// case (`epoch == i16::MAX`) requires a new `InitProducerIdRequest`.
/// We call the broker on every bump, which trades a round-trip for
/// simpler state management; the broker dedups on `(pid, epoch,
/// base_sequence)` regardless, so correctness is unaffected.
pub(crate) async fn request_epoch_bump(
    client: &Client,
    current_pid: i64,
    current_epoch: i16,
) -> Result<(i64, i16)> {
    send_init(client, current_pid, current_epoch).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::protocol::StrBytes;

    fn topic(s: &'static str) -> TopicName {
        TopicName(StrBytes::from_static_str(s))
    }

    #[test]
    fn increment_sequence_normal_increment_adds() {
        assert_eq!(increment_sequence(0, 0), 0);
        assert_eq!(increment_sequence(0, 5), 5);
        assert_eq!(increment_sequence(100, 10), 110);
    }

    #[test]
    fn increment_sequence_wraps_at_i32_max() {
        // i32::MAX - 1 + 5 should wrap to 3 (matches Java's
        // DefaultRecordBatch.incrementSequence).
        assert_eq!(increment_sequence(i32::MAX - 1, 5), 3);
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX, 0), i32::MAX);
    }

    #[test]
    fn retry_delay_is_zero_for_fresh_batch() {
        // attempt == 0 means "no retries yet" — dispatch immediately,
        // don't impose a backoff on the first send.
        assert_eq!(
            retry_delay(0, Duration::from_millis(100), Duration::from_secs(1)),
            Duration::ZERO,
        );
    }

    /// Inclusive band that `retry_delay(attempt)` must fall inside.
    /// Centres on `base * 2^(attempt-1)` (no cap) widened by ±20 %.
    fn jitter_band(attempt: u32, base: Duration) -> (Duration, Duration) {
        let shift = attempt.saturating_sub(1).min(31);
        let nominal = base.as_nanos() * (1u128 << shift);
        let span = nominal / 5;
        (
            Duration::from_nanos((nominal - span) as u64),
            Duration::from_nanos((nominal + span) as u64),
        )
    }

    #[test]
    fn retry_delay_grows_exponentially_within_jitter_band() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(10); // wide cap so jitter isn't truncated
        // Repeat each draw a few times — a single ±20 % roll could
        // happen to land near the centre, so we need several samples
        // to assert the jitter band is actually being exercised.
        for &attempt in &[1u32, 2, 3, 4] {
            let (lo, hi) = jitter_band(attempt, base);
            for _ in 0..32 {
                let d = retry_delay(attempt, base, max);
                assert!(
                    d >= lo && d <= hi,
                    "retry_delay({attempt}) = {d:?} outside [{lo:?}, {hi:?}]",
                );
            }
        }
    }

    #[test]
    fn retry_delay_caps_at_max_even_with_jitter() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        // attempt == 5 → nominal 1600 ms (already > cap). Even the
        // high-side +20 % jitter must not push the result past `max`.
        for _ in 0..64 {
            let d = retry_delay(5, base, max);
            assert!(d <= max, "{d:?} exceeds cap {max:?}");
        }
        // Very large attempt: the shift saturates at 31; result must
        // still be ≤ max.
        for _ in 0..64 {
            let d = retry_delay(100, base, max);
            assert!(d <= max, "{d:?} exceeds cap {max:?}");
        }
    }

    #[test]
    fn bump_attempts_arms_next_attempt_at() {
        let s = IdempotentState::new();
        s.force_ready_for_test(1, 0);
        let _ = s.assign(&topic("t"), PartitionId(0), 3).unwrap();
        let registered_at = Instant::now();
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 3,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: registered_at,
            attempt: 0,
            next_attempt_at: registered_at,
        });

        let base = Duration::from_millis(100);
        let max = Duration::from_secs(10);

        // First bump: attempt 0 -> 1, backoff in [0.8*base, 1.2*base].
        let before = Instant::now();
        s.bump_attempt_one(&topic("t"), PartitionId(0), base, max);
        let head = s.snapshot_inflight().into_iter().next().unwrap();
        assert_eq!(head.attempt, 1);
        let delay = head.next_attempt_at.saturating_duration_since(before);
        let (lo1, hi1) = (base.mul_f64(0.8), base.mul_f64(1.2));
        // Allow a small slack on the high side for the time elapsed
        // between `before` and the bump's internal `Instant::now()`.
        assert!(
            delay >= lo1 && delay <= hi1 + Duration::from_millis(50),
            "expected ≈ base ±20% after first bump, got {delay:?} (band {lo1:?}..{hi1:?})",
        );

        // Second bump: attempt 1 -> 2, backoff in [0.8*2*base, 1.2*2*base].
        let before = Instant::now();
        s.bump_attempt_one(&topic("t"), PartitionId(0), base, max);
        let head = s.snapshot_inflight().into_iter().next().unwrap();
        assert_eq!(head.attempt, 2);
        let delay = head.next_attempt_at.saturating_duration_since(before);
        let (lo2, hi2) = ((base * 2).mul_f64(0.8), (base * 2).mul_f64(1.2));
        assert!(
            delay >= lo2 && delay <= hi2 + Duration::from_millis(50),
            "expected ≈ 2*base ±20% after second bump, got {delay:?} (band {lo2:?}..{hi2:?})",
        );

        // Many more bumps with a tight cap: the result must never
        // exceed the cap, even after the +20 % jitter.
        let tight_max = Duration::from_secs(1);
        for _ in 0..10 {
            s.bump_attempt_one(&topic("t"), PartitionId(0), base, tight_max);
        }
        let before = Instant::now();
        s.bump_attempt_one(&topic("t"), PartitionId(0), base, tight_max);
        let head = s.snapshot_inflight().into_iter().next().unwrap();
        let delay = head.next_attempt_at.saturating_duration_since(before);
        assert!(
            delay <= tight_max + Duration::from_millis(50),
            "expected cap at tight_max after many bumps, got {delay:?}",
        );
    }

    #[test]
    fn bump_attempts_many_keys_each_armed() {
        let s = IdempotentState::new();
        s.force_ready_for_test(1, 0);
        for p in 0..3 {
            let _ = s.assign(&topic("t"), PartitionId(p), 1).unwrap();
            s.register_inflight(InFlightBatch {
                topic: topic("t"),
                partition: PartitionId(p),
                base_sequence: 0,
                record_count: 1,
                producer_id: 1,
                producer_epoch: 0,
                encoded: Bytes::new(),
                waiters: Vec::new(),
                first_send_at: Instant::now(),
                attempt: 0,
                next_attempt_at: Instant::now(),
            });
        }
        let keys: Vec<_> = (0..3)
            .map(|p| (topic("t"), PartitionId(p), 0i32))
            .collect();
        let before = Instant::now();
        s.bump_attempts(&keys, Duration::from_millis(100), Duration::from_secs(1));
        for snap in s.snapshot_inflight() {
            assert_eq!(snap.attempt, 1);
            assert!(snap.next_attempt_at > before);
        }
    }

    #[test]
    fn assign_on_uninitialized_returns_not_ready() {
        let s = IdempotentState::new();
        let err = s.assign(&topic("t"), PartitionId(0), 1).unwrap_err();
        assert_eq!(err, AssignError::NotReady);
    }

    #[test]
    fn assign_advances_next_sequence_per_partition() {
        let s = IdempotentState::new();
        // Inject a Ready state so we don't need a real broker.
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 42,
            producer_epoch: 3,
        };

        let a = s.assign(&topic("t"), PartitionId(0), 4).unwrap();
        assert_eq!(a.producer_id, 42);
        assert_eq!(a.producer_epoch, 3);
        assert_eq!(a.base_sequence, 0);

        let b = s.assign(&topic("t"), PartitionId(0), 7).unwrap();
        assert_eq!(b.base_sequence, 4);

        // Distinct partitions don't share the sequence space.
        let c = s.assign(&topic("t"), PartitionId(1), 2).unwrap();
        assert_eq!(c.base_sequence, 0);
    }

    #[test]
    fn assign_blocked_when_pending_bump() {
        let s = IdempotentState::new();
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 1,
            producer_epoch: 0,
        };
        s.request_partition_bump(&topic("t"), PartitionId(0), BumpMode::Eager);
        let err = s.assign(&topic("t"), PartitionId(0), 1).unwrap_err();
        assert_eq!(err, AssignError::PartitionBlocked);
        // Other partitions still go through.
        assert!(s.assign(&topic("t"), PartitionId(1), 1).is_ok());
    }

    #[test]
    fn fatal_blocks_assign() {
        let s = IdempotentState::new();
        s.mark_fatal("fenced".into());
        let err = s.assign(&topic("t"), PartitionId(0), 1).unwrap_err();
        assert_eq!(err, AssignError::Fatal);
    }

    #[test]
    fn complete_advances_last_acked_and_pops_head() {
        let s = IdempotentState::new();
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 1,
            producer_epoch: 0,
        };
        let _ = s.assign(&topic("t"), PartitionId(0), 3).unwrap();
        let _ = s.assign(&topic("t"), PartitionId(0), 2).unwrap();

        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 3,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 3,
            record_count: 2,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });

        let popped = s.complete(&topic("t"), PartitionId(0), 0).unwrap();
        assert_eq!(popped.base_sequence, 0);
        let inner = s.inner.lock().unwrap();
        let p = inner.partitions.get(&(topic("t"), PartitionId(0))).unwrap();
        // last record in the popped batch had sequence 0+(3-1) = 2.
        assert_eq!(p.last_acked_sequence, 2);
        assert_eq!(p.in_flight.len(), 1);
        assert_eq!(p.in_flight.front().unwrap().base_sequence, 3);
    }

    /// T2c: the in-flight byte counter tracks `encoded.len()` across
    /// every queued batch — `register_inflight` adds, `complete`
    /// subtracts exactly the popped batch's bytes — so
    /// `Accumulator::reserve` can charge in-flight batches against
    /// `buffer_memory` (N8). A `complete` that finds no matching head
    /// must leave the counter untouched.
    #[test]
    fn inflight_bytes_tracks_register_and_complete() {
        let s = IdempotentState::new();
        s.force_ready_for_test(1, 0);
        assert_eq!(s.inflight_bytes(), 0, "no in-flight batches yet");

        let _ = s.assign(&topic("t"), PartitionId(0), 3).unwrap();
        let _ = s.assign(&topic("t"), PartitionId(0), 2).unwrap();

        // register_inflight charges each batch's encoded length.
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 3,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::from_static(b"0123456789"), // 10 bytes
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        assert_eq!(s.inflight_bytes(), 10);
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 3,
            record_count: 2,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::from_static(b"abcd"), // 4 bytes
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        assert_eq!(s.inflight_bytes(), 14, "counter sums every queued batch");

        // complete pops the head and uncounts exactly its bytes; the
        // tail batch stays charged.
        let head = s.complete(&topic("t"), PartitionId(0), 0).unwrap();
        assert_eq!(head.base_sequence, 0);
        assert_eq!(
            s.inflight_bytes(),
            4,
            "head's 10 bytes released, tail's 4 remain",
        );

        // A base_sequence mismatch is a no-op pop — the counter must
        // not move.
        assert!(s.complete(&topic("t"), PartitionId(0), 999).is_none());
        assert_eq!(
            s.inflight_bytes(),
            4,
            "a no-op complete leaves the counter alone",
        );

        // Completing the last in-flight batch returns the counter to 0.
        let tail = s.complete(&topic("t"), PartitionId(0), 3).unwrap();
        assert_eq!(tail.base_sequence, 3);
        assert_eq!(s.inflight_bytes(), 0, "all batches drained, counter back to 0");
    }

    #[test]
    fn complete_refuses_out_of_order_ack() {
        let s = IdempotentState::new();
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 1,
            producer_epoch: 0,
        };
        let _ = s.assign(&topic("t"), PartitionId(0), 1).unwrap();
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        // Wrong base_sequence — must keep the head.
        assert!(s.complete(&topic("t"), PartitionId(0), 99).is_none());
        let inner = s.inner.lock().unwrap();
        let p = inner.partitions.get(&(topic("t"), PartitionId(0))).unwrap();
        assert_eq!(p.in_flight.len(), 1);
        assert_eq!(p.last_acked_sequence, NO_LAST_ACKED);
    }

    #[test]
    fn pending_bumps_take_clears() {
        let s = IdempotentState::new();
        assert!(!s.has_pending_bumps());
        s.request_partition_bump(&topic("t"), PartitionId(0), BumpMode::Eager);
        s.request_partition_bump(&topic("u"), PartitionId(2), BumpMode::Deferred);
        assert!(s.has_pending_bumps());
        let mut taken = s.take_pending_bumps();
        taken.sort_by(|a, b| a.0.0.as_str().cmp(b.0.0.as_str()));
        assert_eq!(
            taken,
            vec![
                ((topic("t"), PartitionId(0)), BumpMode::Eager),
                ((topic("u"), PartitionId(2)), BumpMode::Deferred),
            ]
        );
        assert!(!s.has_pending_bumps());
    }

    /// `Eager` upgrades a prior `Deferred` request for the same
    /// partition; `Deferred` never downgrades an `Eager` one — an eager
    /// request means the broker provably rejected the head, so an
    /// immediate whole-queue re-stamp stays sound (`upsert_pending_bump`).
    #[test]
    fn eager_bump_request_upgrades_deferred() {
        let s = IdempotentState::new();
        s.request_partition_bump(&topic("t"), PartitionId(0), BumpMode::Deferred);
        s.request_partition_bump(&topic("t"), PartitionId(0), BumpMode::Eager);
        assert_eq!(
            s.take_pending_bumps(),
            vec![((topic("t"), PartitionId(0)), BumpMode::Eager)],
        );

        // The reverse order does not downgrade.
        s.request_partition_bump(&topic("u"), PartitionId(1), BumpMode::Eager);
        s.request_partition_bump(&topic("u"), PartitionId(1), BumpMode::Deferred);
        assert_eq!(
            s.take_pending_bumps(),
            vec![((topic("u"), PartitionId(1)), BumpMode::Eager)],
        );
    }

    /// A [`BumpMode::Deferred`] reset parks a fresh `(pid, epoch)` while
    /// the old-epoch in-flight queue is non-empty: `assign` stays
    /// blocked, the in-flight head's encoded bytes are untouched, and
    /// the partition reports as drain-blocked. The moment the last
    /// old-epoch batch settles via `complete`, the reset applies — the
    /// partition swaps to the new `(pid, epoch)`, rewinds
    /// `next_sequence` to 0, and unblocks. Mirrors Java's
    /// `maybeUpdateProducerIdAndEpoch` `!hasInflightBatches` guard.
    #[test]
    fn deferred_reset_parks_then_applies_when_inflight_drains() {
        let s = IdempotentState::new();
        s.force_ready_for_test(1, 0);
        let t = topic("deferred");
        let p = PartitionId(0);

        // One in-flight batch under the original (pid=1, epoch=0).
        let _ = s.assign(&t, p, 3).unwrap();
        let original = Bytes::from_static(b"old-epoch-frame");
        let original_ptr = original.as_ptr();
        s.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p,
            base_sequence: 0,
            record_count: 3,
            producer_id: 1,
            producer_epoch: 0,
            encoded: original,
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 1,
            next_attempt_at: Instant::now(),
        });

        // Park a deferred reset to (pid=99, epoch=7).
        s.begin_deferred_epoch_reset(&t, p, 99, 7);

        // The in-flight head is byte-for-byte unchanged — a deferred
        // reset must never re-stamp a batch that may already be on the
        // broker log.
        let head = s.snapshot_inflight().into_iter().next().unwrap();
        assert_eq!(head.encoded.as_ptr(), original_ptr, "head must not be rewritten");
        assert_eq!(head.base_sequence, 0);

        // `assign` is blocked and the partition is drain-blocked while
        // the reset is parked.
        assert_eq!(
            s.assign(&t, p, 1).unwrap_err(),
            AssignError::PartitionBlocked,
        );
        assert!(s.drain_blocked_partitions().contains(&(t.clone(), p)));

        // Settle the last old-epoch batch — the queue empties, so the
        // parked reset fires.
        let _ = s.complete(&t, p, 0).expect("head completes");

        // Reset applied: new (pid, epoch), next_sequence rewound to 0.
        let assigned = s.assign(&t, p, 2).expect("partition unblocked after drain");
        assert_eq!(assigned.producer_id, 99);
        assert_eq!(assigned.producer_epoch, 7);
        assert_eq!(assigned.base_sequence, 0);
        assert!(!s.drain_blocked_partitions().contains(&(t, p)));
    }

    /// If a partition's old-epoch in-flight queue has *already* drained
    /// by the time the bump pass runs `begin_deferred_epoch_reset`, the
    /// reset is applied immediately rather than parked — there is
    /// nothing accepted-but-unconfirmed left to wait for.
    #[test]
    fn deferred_reset_applies_immediately_when_inflight_already_empty() {
        let s = IdempotentState::new();
        s.force_ready_for_test(1, 0);
        let t = topic("deferred-empty");
        let p = PartitionId(0);
        // Touch the partition entry (empty in-flight queue).
        let _ = s.assign(&t, p, 4).unwrap();

        s.begin_deferred_epoch_reset(&t, p, 55, 3);

        // No parked reset — `assign` is immediately unblocked under the
        // new state with `next_sequence` rewound to 0.
        assert!(!s.drain_blocked_partitions().contains(&(t.clone(), p)));
        let assigned = s.assign(&t, p, 1).expect("unblocked");
        assert_eq!(assigned.producer_id, 55);
        assert_eq!(assigned.producer_epoch, 3);
        assert_eq!(assigned.base_sequence, 0);
    }

    /// While a deferred reset is parked, a further bump request (eager
    /// or deferred) is a no-op: the partition already has a fresh epoch
    /// acquired, and its old-epoch in-flight batches must keep
    /// retransmitting under the old epoch until the queue drains.
    #[test]
    fn bump_request_is_noop_while_deferred_reset_parked() {
        let s = IdempotentState::new();
        s.force_ready_for_test(1, 0);
        let t = topic("noop");
        let p = PartitionId(0);
        let _ = s.assign(&t, p, 1).unwrap();
        s.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p,
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::from_static(b"x"),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        s.begin_deferred_epoch_reset(&t, p, 2, 1);

        s.request_partition_bump(&t, p, BumpMode::Eager);
        s.request_partition_bump(&t, p, BumpMode::Deferred);
        assert!(
            !s.has_pending_bumps(),
            "a bump request must be a no-op while a deferred reset is parked",
        );
    }

    #[test]
    fn predecessor_count_returns_none_when_missing() {
        let s = IdempotentState::new();
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 1,
            producer_epoch: 0,
        };
        let _ = s.assign(&topic("t"), PartitionId(0), 6).unwrap();
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 5,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        assert_eq!(
            s.partition_inflight_predecessor_count(&topic("t"), PartitionId(0), 99),
            None,
        );
        // Unknown partition is also `None`, not `Some(0)`.
        assert_eq!(
            s.partition_inflight_predecessor_count(&topic("u"), PartitionId(0), 0),
            None,
        );
    }

    #[test]
    fn predecessor_count_returns_zero_at_head() {
        let s = IdempotentState::new();
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 1,
            producer_epoch: 0,
        };
        let _ = s.assign(&topic("t"), PartitionId(0), 6).unwrap();
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 5,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        assert_eq!(
            s.partition_inflight_predecessor_count(&topic("t"), PartitionId(0), 5),
            Some(0),
        );
    }

    #[test]
    fn predecessor_count_returns_index_when_present() {
        let s = IdempotentState::new();
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 1,
            producer_epoch: 0,
        };
        let _ = s.assign(&topic("t"), PartitionId(0), 11).unwrap();
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 5,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 10,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::new(),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        assert_eq!(
            s.partition_inflight_predecessor_count(&topic("t"), PartitionId(0), 10),
            Some(1),
        );
    }

    #[tokio::test]
    async fn pop_outside_success_flags_pending_bump() {
        let s = IdempotentState::new();
        s.force_ready_for_test(42, 3);
        let _ = s.assign(&topic("t"), PartitionId(0), 4).unwrap();
        let (tx, rx) = oneshot::channel();
        s.register_inflight(InFlightBatch {
            topic: topic("t"),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 4,
            producer_id: 42,
            producer_epoch: 3,
            encoded: Bytes::new(),
            waiters: vec![tx],
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });

        s.fail_inflight_head(&topic("t"), PartitionId(0), BumpMode::Deferred, || {
            Error::RequestTimeout("synthetic-delivery-timeout".into())
        });

        // The bump flag must be set so a follow-up assign cannot slip
        // through with a poisoned base_sequence.
        assert!(s.has_pending_bumps());
        assert_eq!(
            s.assign(&topic("t"), PartitionId(0), 1).unwrap_err(),
            AssignError::PartitionBlocked,
        );

        // The waiter learns about the failure (no parking forever).
        let outcome = rx.await.expect("waiter must be notified");
        match outcome {
            Err(Error::RequestTimeout(msg)) => {
                assert_eq!(msg, "synthetic-delivery-timeout");
            }
            other => panic!("expected RequestTimeout, got {other:?}"),
        }
    }

    #[test]
    fn fail_inflight_head_on_empty_partition_does_not_flag_bump() {
        let s = IdempotentState::new();
        s.force_ready_for_test(1, 0);
        // Touch the partition entry so it exists but has no in-flight.
        let _ = s.assign(&topic("t"), PartitionId(0), 0).unwrap();

        s.fail_inflight_head(&topic("t"), PartitionId(0), BumpMode::Deferred, || {
            Error::RequestTimeout("nothing to pop".into())
        });

        // No head to fail → no spurious bump request.
        assert!(!s.has_pending_bumps());
    }

    #[test]
    fn sequence_wrap_around_max() {
        let s = IdempotentState::new();
        s.inner.lock().unwrap().init = InitState::Ready {
            producer_id: 1,
            producer_epoch: 0,
        };
        // Force the partition into a near-wrap state.
        let _ = s.assign(&topic("t"), PartitionId(0), 0);
        {
            let mut inner = s.inner.lock().unwrap();
            let p = inner
                .partitions
                .get_mut(&(topic("t"), PartitionId(0)))
                .unwrap();
            p.next_sequence = i32::MAX - 1;
        }
        let a = s.assign(&topic("t"), PartitionId(0), 5).unwrap();
        assert_eq!(a.base_sequence, i32::MAX - 1);
        let b = s.assign(&topic("t"), PartitionId(0), 1).unwrap();
        // (i32::MAX - 1 + 5) wraps to 3, then +1 = 4.
        assert_eq!(b.base_sequence, 3);
    }
}
