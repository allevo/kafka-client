use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::client::PartitionId;
use crate::error::{Error, Result};
use crate::producer::RecordMetadata;
use crate::producer::{BufferFullPolicy, ProducerConfig};
use crate::producer::batch::{
    AppendOutcome, BatchProducerState, FreezeError, FrozenBatch, PartitionBatch, RecordPayload,
    estimate_record_size,
};
use crate::producer::idempotent::IdempotentState;


/// A frozen batch ready to ship, paired with the routing key needed to build
/// a `ProduceRequest`.
pub(crate) struct ReadyBatch {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub frozen: FrozenBatch,
}

impl ReadyBatch {
    /// Split into the two halves the sender consumes independently:
    /// [`EncodedBatch`] is moved into the outgoing `ProduceRequest`,
    /// [`PendingBatch`] stays alive until the broker response settles
    /// the per-record waiters. Splitting eliminates the `Bytes::clone`
    /// of the encoded payload in the request-build path.
    pub(crate) fn split(self) -> (EncodedBatch, PendingBatch) {
        let ReadyBatch {
            topic,
            partition,
            frozen,
        } = self;
        let FrozenBatch {
            encoded,
            waiters,
            record_count,
            state,
            ..
        } = frozen;
        let encoded_batch = EncodedBatch {
            topic: topic.clone(),
            partition,
            encoded,
        };
        let pending = PendingBatch {
            topic,
            partition,
            waiters,
            record_count,
            state,
        };
        (encoded_batch, pending)
    }

    /// Idempotent split that hands back a `Bytes`-clone of the encoded
    /// payload alongside the pending half, so the sender can stash the
    /// bytes in the in-flight queue and still ship them on the wire.
    /// The shared `Bytes` ref stays the source-of-truth for both the
    /// outgoing wire write and the in-flight queue's retransmit copy;
    /// an epoch-bump rewrite (slice C1) operates on a `BytesMut` clone
    /// of these bytes via the header-rewrite helper, so we don't need
    /// to retain the decoded record payloads here.
    pub(crate) fn split_keeping_encoded(self) -> (EncodedBatch, PendingBatch, Bytes) {
        let ReadyBatch {
            topic,
            partition,
            frozen,
        } = self;
        let FrozenBatch {
            encoded,
            waiters,
            record_count,
            state,
            ..
        } = frozen;
        let kept = encoded.clone();
        let encoded_batch = EncodedBatch {
            topic: topic.clone(),
            partition,
            encoded,
        };
        let pending = PendingBatch {
            topic,
            partition,
            waiters,
            record_count,
            state,
        };
        (encoded_batch, pending, kept)
    }
}

/// The wire-format half of a [`ReadyBatch`]. Owned by `send_to_leader`
/// only long enough to be moved into a `PartitionProduceData`.
pub(crate) struct EncodedBatch {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub encoded: Bytes,
}

/// A batch that was popped from the accumulator but whose `freeze`
/// failed. Carries the per-record `waiters` so the sender can resolve
/// them with the real `error` instead of letting the oneshot senders
/// drop (F4), and `(topic, partition)` so the idempotent path can flag
/// the partition for the epoch bump that repairs its sequence gap (N1).
pub(crate) struct FreezeFailure {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    pub error: Error,
}

/// Outcome of a drain pass. `ready` holds the batches that froze
/// cleanly and can ship; `failed` holds any whose `freeze` failed.
/// A freeze failure is rare — an encode error, or a
/// compression-amplified frame over `max_request_size` — but it must
/// not short-circuit the whole drain: one bad batch must not strand
/// the waiters of the batches that froze fine, and a dropped batch's
/// own waiters must still be resolved with the real cause.
pub(crate) struct DrainOutcome {
    pub ready: Vec<ReadyBatch>,
    pub failed: Vec<FreezeFailure>,
}

/// The waiter half of a [`ReadyBatch`]. Held across the broker
/// round-trip so `settle_response` can match per-partition responses
/// back to the records that produced them.
pub(crate) struct PendingBatch {
    pub topic: TopicName,
    pub partition: PartitionId,
    pub waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    pub record_count: usize,
    pub state: BatchProducerState,
}

struct Inner {
    /// FIFO queue of open batches per (topic, partition). `append` only
    /// ever writes to the tail and pushes a fresh tail once the current
    /// one fills, so every batch *except* the tail is full — a "closed
    /// predecessor". A deque-of-one collapses to the pre-rotation
    /// behaviour, which keeps default (sub-threshold) workloads
    /// bit-for-bit identical.
    batches: HashMap<(TopicName, PartitionId), VecDeque<PartitionBatch>>,
    /// Sum of every live reservation: `reserve` adds, `release` and the
    /// drain paths subtract. Kept under the same lock as `batches` so a
    /// `reserve` decision and the `append` it guards see a consistent
    /// view. Invariant: `bytes_in_use <= config.buffer_memory`.
    bytes_in_use: usize,
}

/// Per-key FIFO queue of open `PartitionBatch`es. The producer-facing API
/// appends here; the sender task drains via `drain_ready` on each linger
/// tick (and via `drain_all` on flush/close). The wakeup channel returned
/// from `new()` lets the sender skip the linger interval whenever an
/// append fills a batch — without it, a full batch would still wait for
/// the next tick.
pub(crate) struct Accumulator {
    inner: Mutex<Inner>,
    ready_tx: mpsc::UnboundedSender<()>,
    config: Arc<ProducerConfig>,
    /// Signalled whenever the in-use byte count drops (a drain or an
    /// explicit `release`) or the producer closes. Parked `reserve`
    /// callers wait on it. Not sticky — `reserve` subscribes *before*
    /// re-checking the budget to stay lost-wakeup safe.
    ///
    /// TODO(fairness): every signal uses `notify_waiters()`, which wakes
    /// *all* parked `reserve` callers (thundering herd). A drain that
    /// frees one record's worth of budget still wakes all N waiters; at
    /// most a handful re-claim and the rest re-check and re-park —
    /// O(waiters) wasted work per drain pass. Java's `BufferPool` avoids
    /// this with a FIFO `Deque<Condition>` and a single `signal()` per
    /// freed chunk. Acceptable for now (waiters only exist while the
    /// buffer is full, and every `reserve` is for a single record so
    /// there is no large-request starvation), but a fair, wake-one
    /// scheme is the intended future change.
    room_available: Notify,
    /// Set once by `mark_closed` on `Producer::close`/`Drop`. A parked
    /// `reserve` woken by the close-time `notify_waiters` observes this
    /// and returns promptly instead of waiting out `max_block` (N9).
    closed: AtomicBool,
    /// Idempotent in-flight accounting (T2c). `Some` only when the
    /// producer runs with `enable_idempotence`. `reserve` adds
    /// `idem.inflight_bytes()` to `bytes_in_use` when charging a record
    /// against `buffer_memory`, so a batch that has drained out of the
    /// accumulator but is still parked in the per-partition in-flight
    /// queue (awaiting an ack or a retry) keeps occupying the budget —
    /// N8: otherwise a stalled broker hoards memory through retried
    /// batches even with a cap configured. Wired in by
    /// [`Self::attach_idempotent`] before the accumulator is shared.
    idempotent: Option<Arc<IdempotentState>>,
}

impl Accumulator {
    pub(crate) fn new(config: Arc<ProducerConfig>) -> (Self, mpsc::UnboundedReceiver<()>) {
        let (ready_tx, ready_rx) = mpsc::unbounded_channel();
        let acc = Self {
            inner: Mutex::new(Inner {
                batches: HashMap::new(),
                bytes_in_use: 0,
            }),
            ready_tx,
            config,
            room_available: Notify::new(),
            closed: AtomicBool::new(false),
            idempotent: None,
        };
        (acc, ready_rx)
    }

    /// Wire up the idempotent state so `reserve` can charge in-flight
    /// queue bytes against `buffer_memory` (T2c). `Producer::new` calls
    /// this exactly once, before the accumulator is wrapped in an `Arc`
    /// and shared — hence `&mut self`, no interior mutability needed.
    /// A non-idempotent producer never calls it, so `inflight_bytes`
    /// stays a constant zero.
    pub(crate) fn attach_idempotent(&mut self, idem: Arc<IdempotentState>) {
        self.idempotent = Some(idem);
    }

    /// Bytes currently held in the idempotent in-flight queue, charged
    /// against `buffer_memory` alongside `bytes_in_use`. Zero for a
    /// non-idempotent producer — there is no in-flight queue.
    fn inflight_bytes(&self) -> usize {
        self.idempotent
            .as_ref()
            .map_or(0, |idem| idem.inflight_bytes())
    }

    /// Partitions the idempotent recovery state machine says must not be
    /// drained this pass (T3a): one with a pending epoch bump or a
    /// parked deferred reset. Empty for a non-idempotent producer — it
    /// has no recovery state. The `shouldStopDrainBatchesForPartition`
    /// analog; see [`crate::producer::idempotent::PartitionState`].
    fn drain_blocked_partitions(&self) -> HashSet<(TopicName, PartitionId)> {
        self.idempotent
            .as_ref()
            .map_or_else(HashSet::new, |idem| idem.drain_blocked_partitions())
    }

    /// Reserve `size` bytes of `buffer_memory` budget for a record about
    /// to be appended, blocking until the budget is available or
    /// `deadline` elapses. `Producer::send` calls this *before*
    /// resolving metadata and appending — N4: `deadline` is the
    /// `max_block` budget shared with the metadata wait, not a fresh
    /// one. Every caller that reserves but then fails before `append`
    /// must `release` the same `size`, or the budget leaks.
    ///
    /// Returns [`Error::BufferFull`] if the deadline passes with the
    /// buffer still full, or [`Error::Protocol`] if the producer closed
    /// while parked (N9).
    ///
    /// Under [`BufferFullPolicy::Error`] (T3b) this never parks: a
    /// reservation that does not fit returns [`Error::BufferFull`]
    /// immediately and `deadline` is unused.
    pub(crate) async fn reserve(&self, size: usize, deadline: tokio::time::Instant) -> Result<()> {
        loop {
            // Subscribe to the room-available signal BEFORE re-checking
            // the budget: tokio's `Notify` is not sticky, so creating
            // the listener first closes the lost-wakeup window between
            // the check below and the await (same pattern as
            // `IdempotentState::ensure_ready`).
            let notified = self.room_available.notified();
            tokio::pin!(notified);

            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Protocol("producer closed".into()));
            }
            {
                let mut guard = self.inner.lock().unwrap();
                // T2c / N8: in-flight queue bytes count against the same
                // cap as un-drained accumulator bytes — a batch that has
                // drained out of `bytes_in_use` but is still parked in
                // the idempotent in-flight queue (awaiting an ack or a
                // retry) is just as much occupied `buffer_memory` as one
                // still buffered here. `inflight_bytes()` does not take
                // the `inner` lock, so calling it under `guard` is safe.
                if guard.bytes_in_use + self.inflight_bytes() + size
                    <= self.config.buffer_memory
                {
                    guard.bytes_in_use += size;
                    return Ok(());
                }
            }

            // T3b: under `BufferFullPolicy::Error` the caller asked to
            // shed load synchronously rather than wait — fail the moment
            // the reservation does not fit, never park on
            // `room_available`. (The `notified` listener built above is
            // simply dropped on this path.)
            if self.config.buffer_full_policy == BufferFullPolicy::Error {
                return Err(Error::BufferFull(format!(
                    "buffer_memory ({} B) exhausted; {size} B reservation rejected (buffer_full_policy = Error)",
                    self.config.buffer_memory,
                )));
            }

            // Buffer is full. Park until a drain/release frees room or
            // the shared `max_block` deadline elapses.
            tokio::select! {
                _ = &mut notified => {}
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(Error::BufferFull(format!(
                        "buffer_memory ({} B) exhausted; {size} B reservation timed out within max_block",
                        self.config.buffer_memory,
                    )));
                }
            }
        }
    }

    /// Hand `size` bytes back to the `buffer_memory` budget. Used by
    /// `Producer::send` on any failure path between a successful
    /// `reserve` and the matching `append` — without this the reserved
    /// budget would leak (the record never reaches a `PartitionBatch`,
    /// so no drain ever releases it).
    pub(crate) fn release(&self, size: usize) {
        {
            let mut guard = self.inner.lock().unwrap();
            // `saturating_sub` defends the invariant if a release is
            // ever miscounted; in correct use `size` was claimed by a
            // prior `reserve` so the subtraction never saturates.
            guard.bytes_in_use = guard.bytes_in_use.saturating_sub(size);
        }
        // TODO(fairness): wakes *all* parked reservers, not just one —
        // see the `room_available` field doc.
        self.room_available.notify_waiters();
    }

    /// Mark the accumulator closed and wake every parked `reserve` so a
    /// `Producer::send` blocked on the buffer cap returns promptly
    /// instead of waiting out `max_block` (N9).
    pub(crate) fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
        self.room_available.notify_waiters();
    }

    /// Wake every parked `reserve` caller so it re-checks the
    /// `buffer_memory` budget. The sender calls this after settling a
    /// produce response (T2c / N8): an ack or a terminal failure drops
    /// a batch out of the idempotent in-flight queue, freeing budget
    /// that `reserve`'s check now sees. Idempotent and cheap — a
    /// spurious call just makes the woken reservers re-check and re-park.
    pub(crate) fn notify_room(&self) {
        self.room_available.notify_waiters();
    }

    /// Append `payload` to the tail batch for `(topic, partition)`.
    ///
    /// A record whose estimated wire size exceeds `config.max_record_size`
    /// is rejected with [`Error::RecordTooLarge`] before anything is
    /// touched — no deque entry, no `PartitionBatch`, no oneshot waiter —
    /// so an oversized record can never orphan a waiter or (on the
    /// idempotent path) advance per-partition sequence state. This is the
    /// append-time half of T1b's size guard; `PartitionBatch::freeze`
    /// holds the post-encode half.
    ///
    /// Rotation: a fresh tail batch is pushed when the deque is empty or
    /// when the current tail has already crossed `batch_size_bytes` — so
    /// the tail `append` writes to always still has room. The just-closed
    /// predecessor stays queued in front of the new tail and drains ahead
    /// of it (FIFO, see `drain_ready`).
    ///
    /// When this append fills the tail, push exactly one wakeup so the
    /// sender drains the now-closed batch without waiting for the linger
    /// tick — one wakeup per batch that fills, i.e. one per rotation.
    pub(crate) fn append(
        &self,
        topic: TopicName,
        partition: PartitionId,
        payload: RecordPayload,
    ) -> Result<AppendOutcome> {
        let estimated_size =
            estimate_record_size(payload.key.as_ref(), payload.value.as_ref(), &payload.headers);
        if estimated_size > self.config.max_record_size {
            return Err(Error::RecordTooLarge {
                size: estimated_size,
                limit: self.config.max_record_size,
            });
        }

        let mut guard = self.inner.lock().unwrap();
        let key = (topic, partition);
        let deque = guard.batches.entry(key).or_default();
        // Rotate before appending so the tail always has room: push a
        // fresh batch when the deque is empty or the tail already filled.
        // After this the tail is guaranteed not-full, so the `is_full`
        // below is unambiguously a fresh full transition.
        if deque.back().is_none_or(|tail| tail.is_full()) {
            deque.push_back(PartitionBatch::new(Instant::now()));
        }
        let tail = deque.back_mut().expect("tail just ensured present");
        let outcome = tail.append(payload, self.config.batch_size_bytes);
        let became_full = outcome.is_full;
        // Release the lock before notifying, so the receiver side can
        // acquire the same lock.
        drop(guard);

        if became_full {
            // Send-error means the receiver was dropped — the sender task is
            // gone. Append still succeeded; the batch will be picked up by
            // whatever drain path the caller still has (`drain_all` on
            // close, or nothing if the producer is shutting down).
            let _ = self.ready_tx.send(());
        }
        Ok(outcome)
    }

    /// Freeze and return every batch that is ready to ship: all closed
    /// predecessors (full by construction), plus the tail once it has
    /// crossed the size threshold or its age has reached `config.linger`.
    /// Drained batches are popped from the front of their deque so
    /// per-partition FIFO order holds; a deque that empties is removed so
    /// a later `append` starts fresh.
    ///
    /// Closed predecessors drain on the same pass regardless of linger —
    /// they are already full and have no reason to wait behind the tick.
    ///
    /// `state_for` supplies the `BatchProducerState` per `(topic,
    /// partition, record_count)` so the sender can stamp idempotent
    /// `(producer_id, producer_epoch, base_sequence)` fields without
    /// re-traversing the per-partition state. It fires once per popped
    /// batch, in deque (FIFO) order, so idempotent `assign` advances
    /// densely across rotated batches.
    ///
    /// Returns a [`DrainOutcome`]: cleanly frozen batches in `ready`,
    /// plus any whose `freeze` failed in `failed` (see `freeze_all`).
    pub(crate) fn drain_ready(
        &self,
        now: Instant,
        state_for: impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState,
    ) -> DrainOutcome {
        // T3a: a partition with a pending epoch bump or a parked
        // deferred reset must keep its buffered batches un-drained — a
        // fresh seq-0 batch under a new epoch must not be written ahead
        // of unresolved old-epoch in-flight batches (the
        // `shouldStopDrainBatchesForPartition` analog). Snapshot the
        // blocked set *before* taking the `inner` lock so the
        // idempotent-state lock is never nested under it.
        let blocked = self.drain_blocked_partitions();
        let drained = {
            let mut guard = self.inner.lock().unwrap();
            let linger = self.config.linger;
            let keys: Vec<(TopicName, PartitionId)> = guard.batches.keys().cloned().collect();
            let mut drained: Vec<((TopicName, PartitionId), PartitionBatch)> = Vec::new();
            for key in keys {
                if blocked.contains(&key) {
                    continue;
                }
                let deque = guard.batches.get_mut(&key).expect("key just observed");
                loop {
                    // Pop while the front is shippable. A non-full front
                    // can only be the tail (every predecessor is full by
                    // construction), so the linger check is tail-only.
                    let ready = match deque.front() {
                        Some(front) => {
                            front.is_full() || (deque.len() == 1 && front.age(now) >= linger)
                        }
                        None => break,
                    };
                    if !ready {
                        break;
                    }
                    let batch = deque.pop_front().expect("front just observed");
                    drained.push((key.clone(), batch));
                }
                if deque.is_empty() {
                    guard.batches.remove(&key);
                }
            }
            // A drained batch's records have left the buffer — hand
            // their reserved budget back under the same lock.
            release_drained(&mut guard, &drained);
            drained
        };
        // One wakeup per drain pass, after the lock is dropped: cheap,
        // and idempotent when the pass drained nothing.
        // TODO(fairness): wakes *all* parked reservers, not just one —
        // see the `room_available` field doc.
        self.room_available.notify_waiters();
        self.freeze_all(drained, state_for)
    }

    /// Freeze and return every open batch unconditionally, in per-partition
    /// FIFO order. Used by `flush` and `close` once the user has signalled
    /// they want everything shipped regardless of linger.
    ///
    /// Returns a [`DrainOutcome`]: cleanly frozen batches in `ready`,
    /// plus any whose `freeze` failed in `failed` (see `freeze_all`).
    pub(crate) fn drain_all(
        &self,
        state_for: impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState,
    ) -> DrainOutcome {
        // Same `shouldStopDrainBatchesForPartition` rule as `drain_ready`
        // (T3a): a drain-blocked partition keeps its buffered batches
        // even on a flush/close drain. The fatal-shutdown path clears
        // all recovery state first (`abort_all_recovery`), so it still
        // flushes everything.
        let blocked = self.drain_blocked_partitions();
        let drained: Vec<((TopicName, PartitionId), PartitionBatch)> = {
            let mut guard = self.inner.lock().unwrap();
            let mut out = Vec::new();
            // `retain` keeps the drain-blocked deques in place and drains
            // every other key out. VecDeque iterates front-to-back, so a
            // key's batches stay FIFO and consecutive — `freeze_all`'s
            // `state_for` (and thus idempotent `assign`) sees them in
            // send order.
            guard.batches.retain(|key, deque| {
                if blocked.contains(key) {
                    return true;
                }
                for batch in std::mem::take(deque) {
                    out.push((key.clone(), batch));
                }
                false
            });
            // Every drained batch left the buffer — release its budget.
            release_drained(&mut guard, &out);
            out
        };
        // TODO(fairness): wakes *all* parked reservers, not just one —
        // see the `room_available` field doc.
        self.room_available.notify_waiters();
        self.freeze_all(drained, state_for)
    }

    /// True iff there are zero buffered records. The sender uses this on
    /// flush to decide whether the accumulator side has fully drained.
    /// Drain paths remove emptied deques, so this is normally just "no
    /// keys" — the per-deque check keeps it correct if an empty deque
    /// ever lingers.
    pub(crate) fn is_empty(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .batches
            .values()
            .all(VecDeque::is_empty)
    }

    /// Freeze each drained batch in FIFO order, partitioning the results
    /// into [`DrainOutcome::ready`] and [`DrainOutcome::failed`].
    ///
    /// A `freeze` failure does **not** short-circuit the pass: the
    /// remaining batches still freeze and ship, and the failed batch's
    /// waiters travel back out in a [`FreezeFailure`] so the caller can
    /// resolve them rather than dropping the oneshot senders (F4).
    ///
    /// `state_for` runs the idempotent `assign` (when enabled) and must
    /// fire for every drained batch, in FIFO order, *before* its
    /// `freeze` — so a freeze failure leaves the partition's
    /// `next_sequence` already advanced past records the broker never
    /// saw. Closing that gap is the sender's job (it flags an epoch
    /// bump per `FreezeFailure`, N1); the accumulator only surfaces it.
    fn freeze_all(
        &self,
        drained: Vec<((TopicName, PartitionId), PartitionBatch)>,
        mut state_for: impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState,
    ) -> DrainOutcome {
        let mut ready = Vec::with_capacity(drained.len());
        let mut failed = Vec::new();
        for ((topic, partition), batch) in drained {
            let record_count = batch.len();
            let state = state_for(&topic, partition, record_count);
            match batch.freeze(state, self.config.compression, self.config.max_request_size) {
                Ok(frozen) => ready.push(ReadyBatch {
                    topic,
                    partition,
                    frozen,
                }),
                Err(FreezeError { error, waiters }) => failed.push(FreezeFailure {
                    topic,
                    partition,
                    waiters,
                    error,
                }),
            }
        }
        DrainOutcome { ready, failed }
    }
}

/// Subtract the reserved budget of every drained batch from
/// `bytes_in_use`. Called under the `inner` lock as the last step of a
/// drain pass; the caller signals `room_available` once the lock is
/// dropped. `saturating_sub` is belt-and-braces — a drained batch's
/// `reserved_bytes` was claimed by a prior `reserve`, so the
/// subtraction never actually saturates in correct use.
fn release_drained(
    inner: &mut Inner,
    drained: &[((TopicName, PartitionId), PartitionBatch)],
) {
    let released: usize = drained.iter().map(|(_, b)| b.reserved_bytes()).sum();
    inner.bytes_in_use = inner.bytes_in_use.saturating_sub(released);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kafka_protocol::ResponseError;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{Compression, RecordBatchDecoder};
    use std::io::Cursor;
    use std::time::Duration;

    use crate::producer::RecordMetadata;

    fn topic(s: &'static str) -> TopicName {
        TopicName(StrBytes::from_static_str(s))
    }

    fn payload(value: &'static [u8]) -> RecordPayload {
        RecordPayload {
            key: None,
            value: Some(Bytes::from_static(value)),
            timestamp: 1_700_000_000_000,
            headers: IndexMap::new(),
        }
    }

    fn big_payload(n: usize) -> RecordPayload {
        RecordPayload {
            key: None,
            value: Some(Bytes::from(vec![b'x'; n])),
            timestamp: 1_700_000_000_000,
            headers: IndexMap::new(),
        }
    }

    fn default_cfg() -> Arc<ProducerConfig> {
        Arc::new(ProducerConfig::default())
    }

    /// Default `state_for` closure for tests: always non-idempotent.
    fn nonid()
    -> impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState {
        |_, _, _| BatchProducerState::non_idempotent()
    }

    #[test]
    fn append_creates_batch_on_first_record() {
        let (acc, _rx) = Accumulator::new(default_cfg());
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"a"));
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"b"));
        let drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 1, "second append must reuse the first batch");
        assert_eq!(drained[0].frozen.record_count, 2);
    }

    #[test]
    fn append_rejects_record_exceeding_max_record_size() {
        // 256 B cap, ~536 B record (512 B value + 24 B per-record
        // overhead): the append must fail synchronously and leave the
        // accumulator untouched — no deque entry, no waiter allocated.
        let cfg = Arc::new(ProducerConfig::default().with_max_record_size(256));
        let (acc, _rx) = Accumulator::new(cfg);

        match acc.append(topic("t"), PartitionId(0), big_payload(512)) {
            Err(Error::RecordTooLarge { size, limit }) => {
                assert!(size > 256, "reported size {size} must exceed the limit");
                assert_eq!(limit, 256);
            }
            Err(other) => panic!("expected RecordTooLarge, got {other}"),
            Ok(_) => panic!("oversized record must be rejected"),
        }
        assert!(acc.is_empty(), "a rejected record must not be buffered");

        // The producer is not wedged: an in-cap record still appends.
        let _ = acc
            .append(topic("t"), PartitionId(0), payload(b"ok"))
            .expect("an in-cap record still appends");
        assert!(!acc.is_empty());
    }

    #[test]
    fn append_wakeup_fires_on_each_rotation() {
        // batch_size_bytes = 1: every append fills its tail, so every
        // append after the first also rotates — and each rotation event
        // must surface exactly one wakeup.
        let cfg = Arc::new(ProducerConfig::default().with_batch_size_bytes(1));
        let (acc, mut rx) = Accumulator::new(cfg);

        // First append: the deque is empty, a tail is created and
        // immediately fills. Exactly one wakeup.
        let outcome = acc
            .append(topic("t"), PartitionId(0), big_payload(64))
            .expect("append");
        assert!(outcome.is_full);
        assert!(rx.try_recv().is_ok(), "first fill fires a wakeup");
        assert!(rx.try_recv().is_err(), "exactly one wakeup per fill");

        // Second append: the tail is full, so it rotates to a fresh batch
        // which then fills again — its own, separate wakeup.
        let outcome = acc
            .append(topic("t"), PartitionId(0), big_payload(64))
            .expect("append");
        assert!(outcome.is_full);
        assert!(rx.try_recv().is_ok(), "rotation fires its own wakeup");
        assert!(rx.try_recv().is_err(), "exactly one wakeup per rotation");
    }

    #[test]
    fn append_rotates_when_tail_full() {
        // batch_size_bytes = 1 so every ~64 B record fills its batch —
        // every append after the first rotates into a fresh tail.
        let cfg = Arc::new(ProducerConfig::default().with_batch_size_bytes(1));
        let (acc, _rx) = Accumulator::new(cfg);
        for _ in 0..3 {
            let _ = acc.append(topic("t"), PartitionId(0), big_payload(64));
        }
        // Three appends, three full batches, all queued behind one key.
        let drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 3, "each full tail rotates into its own batch");
        for r in &drained {
            assert_eq!(r.frozen.record_count, 1);
        }
    }

    #[test]
    fn drain_ready_flushes_closed_predecessors_before_linger() {
        // batch_size_bytes = 100: a ~149 B `big_payload(64)` fills a batch
        // on its own; an ~89 B small payload does not. So the first two
        // appends produce full closed predecessors and the third lands in
        // a fresh, not-full tail.
        let cfg = Arc::new(
            ProducerConfig::default()
                .with_batch_size_bytes(100)
                .with_linger(Duration::from_millis(100)),
        );
        let (acc, _rx) = Accumulator::new(cfg);
        let _ = acc.append(topic("t"), PartitionId(0), big_payload(64)); // fills B0
        let _ = acc.append(topic("t"), PartitionId(0), big_payload(64)); // rotates, fills B1
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"tail")); // fresh tail B2

        // `now` barely past creation — well under the 100 ms linger. B0
        // and B1 are full so they drain anyway; B2 is the tail and stays.
        let drained = acc.drain_ready(Instant::now(), nonid()).ready;
        assert_eq!(
            drained.len(),
            2,
            "closed predecessors drain regardless of linger; the tail waits",
        );
        assert!(!acc.is_empty(), "the not-full tail is still buffered");

        // Past the linger window: the tail drains too.
        let drained = acc
            .drain_ready(Instant::now() + Duration::from_millis(200), nonid())
            .ready;
        assert_eq!(drained.len(), 1, "tail drains once its age reaches linger");
        assert!(acc.is_empty());
    }

    #[test]
    fn drain_all_preserves_per_partition_record_order() {
        // batch_size_bytes = 1 forces a rotation every record, so we get
        // five single-record batches — drain_all must hand them back in
        // append order.
        let cfg = Arc::new(ProducerConfig::default().with_batch_size_bytes(1));
        let (acc, _rx) = Accumulator::new(cfg);
        let values: [&'static [u8]; 5] = [b"r0", b"r1", b"r2", b"r3", b"r4"];
        for v in values {
            let _ = acc.append(topic("t"), PartitionId(0), payload(v));
        }
        let drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 5);
        let mut seen: Vec<Vec<u8>> = Vec::new();
        for r in &drained {
            let mut cursor = Cursor::new(r.frozen.encoded.as_ref());
            let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode");
            for rec in decoded.records {
                seen.push(rec.value.expect("value present").to_vec());
            }
        }
        let expected: Vec<Vec<u8>> = values.iter().map(|v| v.to_vec()).collect();
        assert_eq!(seen, expected, "records must come back in append order");
    }

    #[test]
    fn assign_advances_densely_across_rotated_batches() {
        use crate::producer::idempotent::IdempotentState;

        // batch_size_bytes = 100: `big_payload(64)` fills a batch on its
        // own (~149 B), small payloads coalesce (~86 B each). The append
        // sequence below produces three rotated batches of 1, 2, 1
        // records.
        let cfg = Arc::new(ProducerConfig::default().with_batch_size_bytes(100));
        let (acc, _rx) = Accumulator::new(cfg);

        // B0: one big record -> full, next append rotates.
        let _ = acc.append(topic("t"), PartitionId(0), big_payload(64));
        // B1: a small record (not full yet) then a big one that fills it.
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"x"));
        let _ = acc.append(topic("t"), PartitionId(0), big_payload(64));
        // B2: one small record -> stays as the not-full tail.
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"y"));

        let idem = IdempotentState::new();
        idem.force_ready_for_test(7, 2);
        let state_for = |topic: &TopicName, partition: PartitionId, count: usize| {
            let a = idem.assign(topic, partition, count).expect("assign");
            BatchProducerState::idempotent(a.producer_id, a.producer_epoch, a.base_sequence)
        };

        let drained = acc.drain_all(state_for).ready;
        assert_eq!(drained.len(), 3, "three rotated batches");

        let counts: Vec<usize> = drained.iter().map(|r| r.frozen.record_count).collect();
        assert_eq!(counts, vec![1, 2, 1], "record counts per rotated batch");

        // Each batch's wire `base_sequence` (record 0's sequence) must run
        // densely: 0, then +count(B0), then +count(B0)+count(B1).
        let base_sequences: Vec<i32> = drained
            .iter()
            .map(|r| {
                let mut cursor = Cursor::new(r.frozen.encoded.as_ref());
                RecordBatchDecoder::decode(&mut cursor).expect("decode").records[0].sequence
            })
            .collect();
        assert_eq!(
            base_sequences,
            vec![0, 1, 3],
            "base_sequence advances densely across rotated batches",
        );

        // And the partition's next_sequence covers every drained record.
        assert_eq!(
            idem.partition_next_sequence_for_test(&topic("t"), PartitionId(0)),
            Some(4),
        );
    }

    #[test]
    fn append_to_distinct_keys_keeps_batches_separate() {
        let (acc, _rx) = Accumulator::new(default_cfg());
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"a"));
        let _ = acc.append(topic("t"), PartitionId(1), payload(b"b"));
        let drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 2);
        let mut parts: Vec<PartitionId> = drained.iter().map(|r| r.partition).collect();
        parts.sort();
        assert_eq!(parts, vec![PartitionId(0), PartitionId(1)]);
        for r in &drained {
            assert_eq!(r.frozen.record_count, 1);
        }
    }

    #[test]
    fn drain_ready_respects_linger() {
        let cfg = Arc::new(ProducerConfig::default().with_linger(Duration::from_millis(50)));
        let (acc, _rx) = Accumulator::new(cfg);
        let t0 = Instant::now();
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"a"));
        // age < linger: nothing ready.
        assert!(acc.drain_ready(t0, nonid()).ready.is_empty());
        // age >> linger: returns the batch.
        let drained = acc
            .drain_ready(t0 + Duration::from_millis(200), nonid())
            .ready;
        assert_eq!(drained.len(), 1);
    }

    #[test]
    fn drain_ready_pulls_full_batches_immediately() {
        let cfg = Arc::new(
            ProducerConfig::default()
                .with_batch_size_bytes(1)
                .with_linger(Duration::from_secs(10)),
        );
        let (acc, _rx) = Accumulator::new(cfg);
        let _ = acc.append(topic("t"), PartitionId(0), big_payload(64));
        // `now` is the wall clock — far short of the 10s linger — so the
        // only reason this drains is the `is_full` short-circuit.
        let drained = acc.drain_ready(Instant::now(), nonid()).ready;
        assert_eq!(
            drained.len(),
            1,
            "full batch must drain regardless of linger"
        );
    }

    #[test]
    fn drain_ready_removes_drained_entries() {
        let cfg = Arc::new(ProducerConfig::default().with_linger(Duration::from_millis(0)));
        let (acc, _rx) = Accumulator::new(cfg);
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"a"));
        let drained = acc
            .drain_ready(Instant::now() + Duration::from_secs(1), nonid())
            .ready;
        assert_eq!(drained.len(), 1);
        assert!(acc.is_empty());

        // A subsequent append against the same key must allocate a fresh
        // batch — record_count starts at 1, not 2.
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"b"));
        let again = acc.drain_all(nonid()).ready;
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].frozen.record_count, 1);
    }

    #[test]
    fn drain_all_returns_everything() {
        // Default 16 KiB batch size — `payload(b"a")` stays well under, a
        // ~20 KiB payload immediately marks the batch full.
        let (acc, _rx) = Accumulator::new(default_cfg());
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"a"));
        let _ = acc.append(topic("t"), PartitionId(1), big_payload(20_000));
        let _ = acc.append(topic("u"), PartitionId(0), payload(b"x"));
        let _ = acc.append(topic("u"), PartitionId(0), payload(b"y"));

        let drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 3, "one entry per (topic, partition) key");
        assert!(acc.is_empty());
    }

    #[tokio::test]
    async fn frozen_batches_carry_waiters() {
        let (acc, _rx) = Accumulator::new(default_cfg());
        let out_a = acc
            .append(topic("t"), PartitionId(0), payload(b"a"))
            .expect("append a");
        let out_b = acc
            .append(topic("t"), PartitionId(0), payload(b"b"))
            .expect("append b");

        let mut drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 1);
        let ready = drained.pop().unwrap();
        assert_eq!(ready.frozen.waiters.len(), 2);

        let t = topic("t");
        for (i, tx) in ready.frozen.waiters.into_iter().enumerate() {
            tx.send(Ok(RecordMetadata {
                topic: t.clone(),
                partition: PartitionId(0),
                offset: i as i64,
                timestamp: None,
            }))
            .expect("receiver still alive");
        }

        let m_a = out_a.rx.await.expect("rx_a").expect("ok");
        let m_b = out_b.rx.await.expect("rx_b").expect("ok");
        assert_eq!(m_a.offset, 0);
        assert_eq!(m_b.offset, 1);
    }

    /// A `tokio::time::Instant` far enough out that a paused-clock test
    /// never reaches it on its own — used where a `reserve` deadline
    /// must not be the thing that ends the wait.
    fn far() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(3600)
    }

    #[tokio::test(start_paused = true)]
    async fn reserve_blocks_when_over_cap_and_wakes_on_drain() {
        // buffer_memory == exactly one record's reservation: the first
        // reserve+append fills it, so any further reserve must park
        // until a drain frees room.
        let p = big_payload(64);
        let est = estimate_record_size(p.key.as_ref(), p.value.as_ref(), &p.headers);
        let cfg = Arc::new(ProducerConfig::default().with_buffer_memory(est));
        let (acc, _rx) = Accumulator::new(cfg);
        let acc = Arc::new(acc);

        acc.reserve(est, far()).await.expect("first reserve fits exactly");
        acc.append(topic("t"), PartitionId(0), p).expect("append");

        // A second reservation has no room and parks.
        let acc2 = acc.clone();
        let handle = tokio::spawn(async move { acc2.reserve(est, far()).await });
        tokio::task::yield_now().await;
        assert!(
            !handle.is_finished(),
            "reserve must park while the buffer is full",
        );

        // Draining the accumulator releases the appended record's
        // reserved bytes and wakes the parked reserve.
        let drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 1);
        assert!(
            handle.await.expect("join").is_ok(),
            "reserve wakes and succeeds once the drain frees room",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reserve_times_out_when_drain_never_happens() {
        let cfg = Arc::new(ProducerConfig::default().with_buffer_memory(100));
        let (acc, _rx) = Accumulator::new(cfg);
        // Fill the buffer; nothing in this test ever drains it.
        acc.reserve(100, far()).await.expect("first reserve fills the buffer");

        // The second reservation parks and, with no drain ever freeing
        // room, must fail at its deadline rather than hang. Under the
        // paused clock the runtime auto-advances to the deadline once
        // this is the only thing left to run.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        match acc.reserve(64, deadline).await {
            Err(Error::BufferFull(_)) => {}
            other => panic!("expected BufferFull, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn reserve_returns_protocol_error_when_producer_closed() {
        let cfg = Arc::new(ProducerConfig::default().with_buffer_memory(100));
        let (acc, _rx) = Accumulator::new(cfg);
        let acc = Arc::new(acc);
        // Fill the buffer so the next reserve has to park.
        acc.reserve(100, far()).await.expect("first reserve fills the buffer");

        let acc2 = acc.clone();
        let handle = tokio::spawn(async move { acc2.reserve(64, far()).await });
        tokio::task::yield_now().await;
        assert!(
            !handle.is_finished(),
            "reserve must park while the buffer is full",
        );

        // Closing the producer must wake the parked reserve promptly
        // with a protocol error (N9) — not leave it to wait out the
        // deadline.
        acc.mark_closed();
        match handle.await.expect("join") {
            Err(Error::Protocol(_)) => {}
            other => panic!("expected Protocol error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn reserve_error_policy_fails_immediately_when_full() {
        // T3b: under `BufferFullPolicy::Error`, a reservation that does
        // not fit returns `BufferFull` on the spot — it must never park
        // on `room_available`, even with a far-future deadline.
        let cfg = Arc::new(
            ProducerConfig::default()
                .with_buffer_memory(100)
                .with_buffer_full_policy(BufferFullPolicy::Error),
        );
        let (acc, _rx) = Accumulator::new(cfg);

        // A reservation that fits still succeeds normally.
        acc.reserve(100, far())
            .await
            .expect("first reserve fills the buffer");

        // The buffer is now full. `far()` is an hour out, so a returned
        // error proves `reserve` did not park waiting for the deadline —
        // `Error` policy short-circuits before the `select!`.
        match acc.reserve(64, far()).await {
            Err(Error::BufferFull(_)) => {}
            other => panic!("expected immediate BufferFull, got {other:?}"),
        }

        // Once room is freed, `Error` policy reserves again without
        // complaint — the rejection is not a one-way wedge.
        acc.release(100);
        acc.reserve(64, far())
            .await
            .expect("reserve succeeds once the buffer has room again");
    }

    #[test]
    fn compression_threaded_through_config() {
        let cfg = Arc::new(ProducerConfig::default().with_compression(Compression::Gzip));
        let (acc, _rx) = Accumulator::new(cfg);
        // Repetitive payload so gzip actually compresses — guards against
        // an accidental `Compression::None` fallback in the encode path.
        let big = Bytes::from(vec![b'a'; 4096]);
        for _ in 0..4 {
            let p = RecordPayload {
                key: Some(Bytes::from_static(b"k")),
                value: Some(big.clone()),
                timestamp: 1_700_000_000_000,
                headers: IndexMap::new(),
            };
            let _ = acc.append(topic("t"), PartitionId(0), p);
        }

        let drained = acc.drain_all(nonid()).ready;
        assert_eq!(drained.len(), 1);
        let mut cursor = Cursor::new(drained[0].frozen.encoded.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode");
        assert_eq!(decoded.compression, Compression::Gzip);
        assert_eq!(decoded.records.len(), 4);
    }

    #[tokio::test]
    async fn freeze_failure_resolves_waiters_with_error() {
        // `max_request_size = 16` is below even the v2 batch header, so
        // every freeze trips the post-encode size guard. The record
        // still clears the `max_record_size` append check (left at the
        // 1 MiB default), so it buffers fine — the failure is isolated
        // to `freeze`.
        let cfg = Arc::new(ProducerConfig::default().with_max_request_size(16));
        let (acc, _rx) = Accumulator::new(cfg);
        let outcome = acc
            .append(topic("t"), PartitionId(0), payload(b"hello"))
            .expect("append succeeds — only freeze is over the cap");

        let drained = acc.drain_all(nonid());
        // The unfreezable batch must not ship, and — crucially — must
        // not be dropped silently: it surfaces in `failed` carrying the
        // real cause and the live waiter.
        assert!(drained.ready.is_empty(), "an unfreezable batch must not ship");
        assert_eq!(drained.failed.len(), 1);
        let failure = drained.failed.into_iter().next().unwrap();
        assert_eq!(failure.partition, PartitionId(0));
        match failure.error {
            Error::Broker { error } => assert_eq!(error, ResponseError::MessageTooLarge),
            other => panic!("expected Broker(MessageTooLarge), got {other}"),
        }

        // The oneshot half wasn't orphaned: resolving the surfaced
        // waiter reaches the caller's `SendFuture` instead of it seeing
        // a misleading `producer closed`.
        for tx in failure.waiters {
            let _ = tx.send(Err(Error::Broker {
                error: ResponseError::MessageTooLarge,
            }));
        }
        match outcome.rx.await.expect("waiter must not be orphaned") {
            Err(Error::Broker {
                error: ResponseError::MessageTooLarge,
            }) => {}
            other => panic!("expected Broker(MessageTooLarge), got {other:?}"),
        }
    }

    /// T3a: a partition with a parked deferred epoch reset (a stale
    /// `(pid, epoch)` plus a non-empty in-flight queue) must keep its
    /// freshly-appended accumulator batches buffered — the
    /// `shouldStopDrainBatchesForPartition` analog. A sibling partition
    /// with no recovery in progress drains normally on the same pass.
    /// Once the blocked partition's in-flight queue drains, the deferred
    /// reset applies, the partition unblocks, and its buffered batch
    /// drains on the next pass.
    #[tokio::test]
    async fn stale_epoch_partition_does_not_drain_new_batches() {
        use crate::producer::idempotent::{IdempotentState, InFlightBatch};

        let idem = Arc::new(IdempotentState::new());
        idem.force_ready_for_test(1, 0);
        let blocked = topic("blocked");
        let healthy = topic("healthy");

        // `blocked` carries an in-flight batch under the old epoch plus
        // a parked deferred reset → the recovery state machine reports
        // it as drain-blocked.
        let _ = idem.assign(&blocked, PartitionId(0), 1).unwrap();
        idem.register_inflight(InFlightBatch {
            topic: blocked.clone(),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::from_static(b"old-epoch"),
            waiters: Vec::new(),
            first_send_at: Instant::now(),
            attempt: 1,
            next_attempt_at: Instant::now(),
        });
        idem.begin_deferred_epoch_reset(&blocked, PartitionId(0), 2, 1);

        let cfg = Arc::new(ProducerConfig::default().with_linger(Duration::from_millis(0)));
        let (mut acc, _rx) = Accumulator::new(cfg);
        acc.attach_idempotent(idem.clone());

        let _ = acc
            .append(blocked.clone(), PartitionId(0), payload(b"new"))
            .expect("append on blocked partition still succeeds");
        let _ = acc
            .append(healthy.clone(), PartitionId(0), payload(b"ok"))
            .expect("append on healthy partition");

        // `drain_ready` leaves the blocked partition buffered; the
        // healthy one drains.
        let drained = acc
            .drain_ready(Instant::now() + Duration::from_secs(1), nonid())
            .ready;
        assert_eq!(drained.len(), 1, "only the healthy partition drains");
        assert_eq!(drained[0].topic, healthy);
        assert!(!acc.is_empty(), "the blocked partition's batch stays buffered");

        // Settle the in-flight batch → the queue empties → the deferred
        // reset applies → the partition unblocks → its buffered batch
        // now drains.
        let _ = idem
            .complete(&blocked, PartitionId(0), 0)
            .expect("in-flight batch completes");
        let drained = acc
            .drain_ready(Instant::now() + Duration::from_secs(1), nonid())
            .ready;
        assert_eq!(drained.len(), 1, "the unblocked partition now drains");
        assert_eq!(drained[0].topic, blocked);
        assert!(acc.is_empty());
    }
}
