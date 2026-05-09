use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use tokio::sync::{mpsc, oneshot};

use crate::client::PartitionId;
use crate::error::Result;
use crate::producer::ProducerConfig;
use crate::producer::RecordMetadata;
use crate::producer::batch::{
    AppendOutcome, BatchProducerState, FrozenBatch, PartitionBatch, RecordPayload,
};


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
    /// Active open batch per (topic, partition).
    batches: HashMap<(TopicName, PartitionId), PartitionBatch>,
}

/// Per-key buffer of open `PartitionBatch`es. The producer-facing API appends
/// here; the future sender task drains via `drain_ready` on each linger tick
/// (and via `drain_all` on flush/close). The wakeup channel returned from
/// `new()` lets the sender skip the linger interval whenever an append
/// fills a batch — without it, a full batch would still wait for the next
/// tick.
pub(crate) struct Accumulator {
    inner: Mutex<Inner>,
    ready_tx: mpsc::UnboundedSender<()>,
    config: Arc<ProducerConfig>,
}

impl Accumulator {
    pub(crate) fn new(config: Arc<ProducerConfig>) -> (Self, mpsc::UnboundedReceiver<()>) {
        let (ready_tx, ready_rx) = mpsc::unbounded_channel();
        let acc = Self {
            inner: Mutex::new(Inner {
                batches: HashMap::new(),
            }),
            ready_tx,
            config,
        };
        (acc, ready_rx)
    }

    /// Append `payload` to the open batch for `(topic, partition)`, creating
    /// a new `PartitionBatch` if none exists. When this append crosses the
    /// `batch_size_bytes` threshold for the first time, push one wakeup so
    /// the sender can drain without waiting for the linger tick.
    pub(crate) fn append(
        &self,
        topic: TopicName,
        partition: PartitionId,
        payload: RecordPayload,
    ) -> AppendOutcome {
        let mut guard = self.inner.lock().unwrap();
        let key = (topic, partition);
        let batch = guard
            .batches
            .entry(key)
            .or_insert_with(|| PartitionBatch::new(Instant::now()));
        let was_full = batch.is_full();
        let outcome = batch.append(payload, self.config.batch_size_bytes);
        let became_full = outcome.is_full && !was_full;
        // Release the lock before notifying, so the receiver size can acquire the same lock
        drop(guard);

        if became_full {
            // Send-error means the receiver was dropped — the sender task is
            // gone. Append still succeeded; the batch will be picked up by
            // whatever drain path the caller still has (`drain_all` on
            // close, or nothing if the producer is shutting down).
            let _ = self.ready_tx.send(());
        }
        outcome
    }

    /// Freeze and return every batch whose age has reached `config.linger` or
    /// which already crossed the size threshold. Drained entries are removed,
    /// so a subsequent `append` to the same key starts a fresh batch.
    ///
    /// `state_for` supplies the `BatchProducerState` per `(topic,
    /// partition, record_count)` so the sender can stamp idempotent
    /// `(producer_id, producer_epoch, base_sequence)` fields without
    /// re-traversing the per-partition state.
    pub(crate) fn drain_ready(
        &self,
        now: Instant,
        state_for: impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState,
    ) -> Result<Vec<ReadyBatch>> {
        let drained = {
            let mut guard = self.inner.lock().unwrap();
            let linger = self.config.linger;
            let keys: Vec<(TopicName, PartitionId)> = guard
                .batches
                .iter()
                .filter(|(_, b)| b.is_full() || b.age(now) >= linger)
                .map(|(k, _)| k.clone())
                .collect();
            keys.into_iter()
                .map(|k| {
                    let b = guard.batches.remove(&k).expect("key just observed");
                    (k, b)
                })
                .collect::<Vec<_>>()
        };
        self.freeze_all(drained, state_for)
    }

    /// Freeze and return every open batch unconditionally. Used by `flush`
    /// and `close` once the user has signalled they want everything shipped
    /// regardless of linger.
    pub(crate) fn drain_all(
        &self,
        state_for: impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState,
    ) -> Result<Vec<ReadyBatch>> {
        let drained: Vec<((TopicName, PartitionId), PartitionBatch)> = {
            let mut guard = self.inner.lock().unwrap();
            guard.batches.drain().collect()
        };
        self.freeze_all(drained, state_for)
    }

    /// True iff there are zero open batches. The sender uses this on flush
    /// to decide whether the accumulator side has fully drained.
    pub(crate) fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().batches.is_empty()
    }

    fn freeze_all(
        &self,
        drained: Vec<((TopicName, PartitionId), PartitionBatch)>,
        mut state_for: impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState,
    ) -> Result<Vec<ReadyBatch>> {
        let mut out = Vec::with_capacity(drained.len());
        for ((topic, partition), batch) in drained {
            let record_count = batch.len();
            let state = state_for(&topic, partition, record_count);
            let frozen = batch.freeze(state, self.config.compression)?;
            out.push(ReadyBatch {
                topic,
                partition,
                frozen,
            });
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
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
        let drained = acc.drain_all(nonid()).expect("drain");
        assert_eq!(drained.len(), 1, "second append must reuse the first batch");
        assert_eq!(drained[0].frozen.record_count, 2);
    }

    #[test]
    fn append_full_signals_wakeup() {
        // batch_size_bytes = 1: every append crosses the threshold.
        let cfg = Arc::new(ProducerConfig::default().with_batch_size_bytes(1));
        let (acc, mut rx) = Accumulator::new(cfg);

        let outcome = acc.append(topic("t"), PartitionId(0), big_payload(64));
        assert!(outcome.is_full);
        assert!(rx.try_recv().is_ok(), "first full transition fires wakeup");
        // Subsequent appends to the same already-full batch must not re-fire.
        let _ = acc.append(topic("t"), PartitionId(0), big_payload(64));
        assert!(
            rx.try_recv().is_err(),
            "is_full transition is one-shot per batch"
        );
    }

    #[test]
    fn append_to_distinct_keys_keeps_batches_separate() {
        let (acc, _rx) = Accumulator::new(default_cfg());
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"a"));
        let _ = acc.append(topic("t"), PartitionId(1), payload(b"b"));
        let drained = acc.drain_all(nonid()).expect("drain");
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
        assert!(acc.drain_ready(t0, nonid()).expect("drain").is_empty());
        // age >> linger: returns the batch.
        let drained = acc
            .drain_ready(t0 + Duration::from_millis(200), nonid())
            .expect("drain");
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
        let drained = acc.drain_ready(Instant::now(), nonid()).expect("drain");
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
            .expect("drain");
        assert_eq!(drained.len(), 1);
        assert!(acc.is_empty());

        // A subsequent append against the same key must allocate a fresh
        // batch — record_count starts at 1, not 2.
        let _ = acc.append(topic("t"), PartitionId(0), payload(b"b"));
        let again = acc.drain_all(nonid()).expect("drain");
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

        let drained = acc.drain_all(nonid()).expect("drain");
        assert_eq!(drained.len(), 3, "one entry per (topic, partition) key");
        assert!(acc.is_empty());
    }

    #[tokio::test]
    async fn frozen_batches_carry_waiters() {
        let (acc, _rx) = Accumulator::new(default_cfg());
        let out_a = acc.append(topic("t"), PartitionId(0), payload(b"a"));
        let out_b = acc.append(topic("t"), PartitionId(0), payload(b"b"));

        let mut drained = acc.drain_all(nonid()).expect("drain");
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

        let drained = acc.drain_all(nonid()).expect("drain");
        assert_eq!(drained.len(), 1);
        let mut cursor = Cursor::new(drained[0].frozen.encoded.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode");
        assert_eq!(decoded.compression, Compression::Gzip);
        assert_eq!(decoded.records.len(), 4);
    }
}
