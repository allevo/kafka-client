use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::producer::RecordMetadata;

pub(crate) struct AppendOutcome {
    /// Resolves when the record is acknowledged.
    pub rx: oneshot::Receiver<Result<RecordMetadata>>,
    /// True once `estimated_size >= max_batch_bytes`.
    pub is_full: bool,
}

/// User-controlled subset of a record.
pub(crate) struct RecordPayload {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub timestamp: i64,
    pub headers: IndexMap<StrBytes, Option<Bytes>>,
}

/// A RecordBatch shares the below fields. See `PartitionBatch::freeze` method.
pub(crate) struct BatchProducerState {
    pub transactional: bool,
    pub control: bool,
    pub partition_leader_epoch: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl Default for BatchProducerState {
    fn default() -> Self {
        Self::non_idempotent()
    }
}

impl BatchProducerState {
    /// Non-idempotent, non-transactional v1 producer. The only variant the
    /// Sender will use until idempotence/transactions land.
    pub(crate) fn non_idempotent() -> Self {
        Self {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
        }
    }
}

/// When it is the time, the `PartitionBatch` is
/// transformed into `FrozenBatch` so it is ready
/// to send to the broker
pub(crate) struct FrozenBatch {
    pub encoded: Bytes,
    pub waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    pub record_count: usize,
    pub first_send_at: Option<Instant>,
    pub attempt: u32,
}

/// Buffer of records for `(topic, partition)` pair.
///
/// It is kept in memory till thresholds (time and size)
/// are met
pub(crate) struct PartitionBatch {
    records: Vec<Record>,
    waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    created_at: Instant,
    first_send_at: Option<Instant>,
    size_estimator: SizeEstimator,
    attempt: u32,
}

impl PartitionBatch {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            records: Vec::new(),
            waiters: Vec::new(),
            created_at: now,
            first_send_at: None,
            size_estimator: SizeEstimator::new(),
            attempt: 0,
        }
    }

    pub(crate) fn append(
        &mut self,
        payload: RecordPayload,
        max_batch_bytes: usize,
    ) -> AppendOutcome {
        // Per-record positional fields are bound here:
        //   - `sequence = offset` keeps `(offset - sequence)` constant so
        //     the `kafka_protocol` encoder packs every record into a single
        //     v2 RecordBatch frame.
        //   - Relative offsets run `0..N` so the broker can rebase against
        //     the base offset it assigns on append.
        let index = self.records.len() as i64;
        let record = Record {
            key: payload.key,
            value: payload.value,
            timestamp: payload.timestamp,
            headers: payload.headers,

            offset: index,
            sequence: index as i32,
            timestamp_type: TimestampType::Creation,

            // overwritten in `freeze()`
            producer_id: 0,
            producer_epoch: 0,
            partition_leader_epoch: 0,
            transactional: false,
            control: false,
        };

        self.size_estimator.add_record_estimation(&record);
        let is_full = self.size_estimator.is_full(max_batch_bytes);

        let (tx, rx) = oneshot::channel();
        self.records.push(record);
        self.waiters.push(tx);

        AppendOutcome { rx, is_full }
    }

    /// Record the first-dispatch timestamp. Idempotent across retries: the
    /// accumulator's requeue path re-invokes this, but we keep the earliest
    /// instant so `delivery_timeout` measures wall-clock from the first
    /// attempt.
    pub(crate) fn mark_dispatched(&mut self, now: Instant) {
        if self.first_send_at.is_none() {
            self.first_send_at = Some(now);
        }
    }

    pub(crate) fn bump_attempt(&mut self) {
        self.attempt += 1;
    }

    pub(crate) fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.created_at)
    }

    /// Encode the batch into a single RecordBatch v2 frame, suitable for
    /// `ProduceRequest.topic_data[_].partition_data[_].records`.
    pub(crate) fn freeze(
        mut self,
        state: BatchProducerState,
        compression: Compression,
    ) -> Result<FrozenBatch> {
        // Force the v2-batch invariants uniformly across every record.
        for r in self.records.iter_mut() {
            r.producer_id = state.producer_id;
            r.producer_epoch = state.producer_epoch;
            r.partition_leader_epoch = state.partition_leader_epoch;
            r.transactional = state.transactional;
            r.control = state.control;
        }

        let options = RecordEncodeOptions {
            version: 2,
            compression,
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(&mut buf, self.records.iter(), &options)
            .map_err(|e| Error::Protocol(format!("record batch encode: {e}")))?;
        Ok(FrozenBatch {
            encoded: buf.freeze(),
            waiters: self.waiters,
            record_count: self.records.len(),
            first_send_at: self.first_send_at,
            attempt: self.attempt,
        })
    }
}

/// Make size estimation for v2 PartitionBatch
struct SizeEstimator(usize);
impl SizeEstimator {
    fn new() -> Self {
        // Header
        const BATCH_HEADER_OVERHEAD: usize = 61;

        Self(BATCH_HEADER_OVERHEAD)
    }

    /// Update self estimation
    fn add_record_estimation(&mut self, record: &Record) {
        const PER_RECORD_OVERHEAD: usize = 24;
        const PER_HEADER_OVERHEAD: usize = 8;

        let mut size = PER_RECORD_OVERHEAD;
        if let Some(k) = record.key.as_ref() {
            size += k.len();
        }
        if let Some(v) = record.value.as_ref() {
            size += v.len();
        }
        for (hk, hv) in &record.headers {
            size += PER_HEADER_OVERHEAD + hk.len();
            if let Some(v) = hv.as_ref() {
                size += v.len();
            }
        }
        self.0 += size
    }

    fn is_full(&self, threshold: usize) -> bool {
        self.0 >= threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::RecordBatchDecoder;
    use std::io::Cursor;

    fn sample_payload(key: Option<&'static [u8]>, value: Option<&'static [u8]>) -> RecordPayload {
        RecordPayload {
            key: key.map(Bytes::from_static),
            value: value.map(Bytes::from_static),
            timestamp: 1_700_000_000_000,
            headers: IndexMap::new(),
        }
    }

    fn big_value_payload(n: usize) -> RecordPayload {
        let mut p = sample_payload(None, None);
        p.value = Some(Bytes::from(vec![b'x'; n]));
        p
    }

    #[test]
    fn append_reports_full_at_threshold() {
        let mut batch = PartitionBatch::new(Instant::now());
        // Each record contributes ~100 + PER_RECORD_OVERHEAD bytes. With a
        // 400-byte limit the batch should cross the threshold on or before
        // the fourth append, and never regress afterwards.
        let max = 400;
        let mut seen_full = false;
        let mut last_was_full = false;
        for _ in 0..8 {
            let outcome = batch.append(big_value_payload(100), max);
            if outcome.is_full {
                seen_full = true;
            }
            if seen_full {
                assert!(
                    outcome.is_full,
                    "is_full must stay true once the threshold is crossed"
                );
            }
            last_was_full = outcome.is_full;
        }
        assert!(seen_full, "threshold never crossed");
        assert!(last_was_full);
    }

    #[test]
    fn append_never_refuses() {
        let mut batch = PartitionBatch::new(Instant::now());
        // Tiny threshold — every append after the first crosses it, but
        // `append` is policy-free and keeps accepting records.
        for i in 0..10 {
            let outcome = batch.append(big_value_payload(64), 1);
            assert!(outcome.is_full, "expected full flag on append {i}");
        }
        assert_eq!(batch.len(), 10);
    }

    #[test]
    fn freeze_round_trips_via_record_batch_decoder() {
        let mut batch = PartitionBatch::new(Instant::now());
        let _ = batch.append(sample_payload(None, Some(b"no-key")), usize::MAX);
        let _ = batch.append(sample_payload(Some(b"k1"), Some(b"v1")), usize::MAX);

        let mut with_header = sample_payload(Some(b"k2"), Some(b"v2"));
        with_header.headers.insert(
            StrBytes::from_static_str("trace-id"),
            Some(Bytes::from_static(b"abc")),
        );
        // Null header value — wire-legal in Kafka v2; round-trip should
        // preserve the `None`.
        with_header
            .headers
            .insert(StrBytes::from_static_str("empty"), None);
        let _ = batch.append(with_header, usize::MAX);

        let frozen = batch
            .freeze(BatchProducerState::non_idempotent(), Compression::None)
            .expect("encode");
        assert_eq!(frozen.record_count, 3);

        let mut cursor = Cursor::new(frozen.encoded.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode");
        assert_eq!(decoded.compression, Compression::None);
        assert_eq!(decoded.version, 2);
        assert_eq!(decoded.records.len(), 3);

        assert_eq!(decoded.records[0].key, None);
        assert_eq!(
            decoded.records[0].value.as_deref(),
            Some(b"no-key".as_ref())
        );
        assert_eq!(decoded.records[1].key.as_deref(), Some(b"k1".as_ref()));
        assert_eq!(decoded.records[1].value.as_deref(), Some(b"v1".as_ref()));
        assert_eq!(decoded.records[2].key.as_deref(), Some(b"k2".as_ref()));
        assert_eq!(decoded.records[2].value.as_deref(), Some(b"v2".as_ref()));
        assert_eq!(decoded.records[2].headers.len(), 2);
        assert_eq!(
            decoded.records[2]
                .headers
                .get(&StrBytes::from_static_str("trace-id"))
                .unwrap()
                .as_deref(),
            Some(b"abc".as_ref())
        );
        assert!(
            decoded.records[2]
                .headers
                .get(&StrBytes::from_static_str("empty"))
                .unwrap()
                .is_none()
        );

        // Relative offsets must be 0..record_count so the broker can assign
        // absolute offsets by adding the assigned base.
        for (i, r) in decoded.records.iter().enumerate() {
            assert_eq!(r.offset, i as i64);
        }
    }

    #[test]
    fn freeze_with_gzip_decodes_back() {
        let mut plain = PartitionBatch::new(Instant::now());
        let mut gzipped = PartitionBatch::new(Instant::now());
        // A payload with lots of repetition so gzip actually shrinks it —
        // guarantees the byte-for-byte comparison below catches an accidental
        // Compression::None fallback.
        let big = Bytes::from(vec![b'a'; 4096]);
        for _ in 0..4 {
            let mk = || RecordPayload {
                key: Some(Bytes::from_static(b"k")),
                value: Some(big.clone()),
                timestamp: 1_700_000_000_000,
                headers: IndexMap::new(),
            };
            let _ = plain.append(mk(), usize::MAX);
            let _ = gzipped.append(mk(), usize::MAX);
        }

        let frozen_plain = plain
            .freeze(BatchProducerState::non_idempotent(), Compression::None)
            .expect("encode plain");
        let frozen_gz = gzipped
            .freeze(BatchProducerState::non_idempotent(), Compression::Gzip)
            .expect("encode gzip");
        assert_ne!(
            frozen_plain.encoded, frozen_gz.encoded,
            "gzip bytes should differ from uncompressed"
        );
        assert!(
            frozen_gz.encoded.len() < frozen_plain.encoded.len(),
            "gzip should shrink a repetitive payload"
        );

        let mut cursor = Cursor::new(frozen_gz.encoded.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode gzip");
        assert_eq!(decoded.compression, Compression::Gzip);
        assert_eq!(decoded.records.len(), 4);
        for r in &decoded.records {
            assert_eq!(r.value.as_ref(), Some(&big));
            assert_eq!(r.key.as_deref(), Some(b"k".as_ref()));
        }
    }

    #[tokio::test]
    async fn waiters_survive_freeze() {
        let mut batch = PartitionBatch::new(Instant::now());
        let out1 = batch.append(sample_payload(None, Some(b"a")), usize::MAX);
        let out2 = batch.append(sample_payload(None, Some(b"b")), usize::MAX);

        let frozen = batch
            .freeze(BatchProducerState::non_idempotent(), Compression::None)
            .expect("encode");
        assert_eq!(frozen.waiters.len(), 2);

        // Sender side (future slice) delivers one RecordMetadata per record.
        let topic = kafka_protocol::messages::TopicName(StrBytes::from_static_str("t"));
        for (i, tx) in frozen.waiters.into_iter().enumerate() {
            tx.send(Ok(RecordMetadata {
                topic: topic.clone(),
                partition: crate::client::PartitionId(0),
                offset: i as i64,
                timestamp: None,
            }))
            .expect("receiver still alive");
        }

        let r1 = out1.rx.await.expect("rx1").expect("ok");
        let r2 = out2.rx.await.expect("rx2").expect("ok");
        assert_eq!(r1.offset, 0);
        assert_eq!(r2.offset, 1);
    }

    #[test]
    fn mark_dispatched_is_idempotent() {
        let mut batch = PartitionBatch::new(Instant::now());
        let first = Instant::now();
        batch.mark_dispatched(first);
        let later = first + Duration::from_secs(5);
        batch.mark_dispatched(later);
        assert_eq!(batch.first_send_at, Some(first));
    }

    #[test]
    fn bump_attempt_increments() {
        let mut batch = PartitionBatch::new(Instant::now());
        assert_eq!(batch.attempt(), 0);
        batch.bump_attempt();
        batch.bump_attempt();
        assert_eq!(batch.attempt(), 2);
    }
}
