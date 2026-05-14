use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kafka_protocol::ResponseError;
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
#[derive(Clone, Copy)]
pub(crate) struct BatchProducerState {
    pub transactional: bool,
    pub control: bool,
    pub partition_leader_epoch: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// First record's sequence id. Successive records are stamped with
    /// `base_sequence.wrapping_add(i)` so the encoder's
    /// `(offset - sequence) == constant` invariant holds. For the
    /// non-idempotent variant this is `0`.
    pub base_sequence: i32,
}

impl Default for BatchProducerState {
    fn default() -> Self {
        Self::non_idempotent()
    }
}

impl BatchProducerState {
    /// Non-idempotent, non-transactional v1 producer. Sequence stays at 0.
    pub(crate) fn non_idempotent() -> Self {
        Self {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            base_sequence: 0,
        }
    }

    /// Idempotent, non-transactional KIP-98 producer. `producer_id` and
    /// `producer_epoch` are returned by the broker on `InitProducerId`;
    /// `base_sequence` is per-partition and assigned monotonically by
    /// [`crate::producer::idempotent::IdempotentState`].
    pub(crate) fn idempotent(producer_id: i64, producer_epoch: i16, base_sequence: i32) -> Self {
        Self {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id,
            producer_epoch,
            base_sequence,
        }
    }
}

/// A [`PartitionBatch::freeze`] that failed *after* the batch was
/// drained out of the accumulator. Carries the per-record `waiters`
/// back to the caller so the drain path can resolve them with `error`
/// (the real cause) instead of dropping the oneshot senders — a
/// dropped sender surfaces to the user as a misleading
/// `Error::Protocol("producer closed")`.
#[derive(Debug)]
pub(crate) struct FreezeError {
    pub error: Error,
    pub waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
}

/// When it is the time, the `PartitionBatch` is
/// transformed into `FrozenBatch` so it is ready
/// to send to the broker
pub(crate) struct FrozenBatch {
    pub encoded: Bytes,
    pub waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    pub record_count: usize,
    /// Producer-state stamped into the encoded bytes. Carried so the
    /// idempotent path can register `(producer_id, producer_epoch,
    /// base_sequence)` into its in-flight queue without re-deriving them.
    pub state: BatchProducerState,
}

/// Buffer of records for `(topic, partition)` pair.
///
/// It is kept in memory till thresholds (time and size)
/// are met
pub(crate) struct PartitionBatch {
    records: Vec<Record>,
    waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    created_at: Instant,
    size_estimator: SizeEstimator,
    full: bool,
    /// Sum of `estimate_record_size` over every appended record — i.e.
    /// the exact `buffer_memory` budget `Producer::send` reserved for
    /// this batch's records. When the accumulator drains the batch, it
    /// releases this many bytes back to the budget, keeping `reserve`
    /// and the drain-time release symmetric (the batch-header overhead
    /// the `SizeEstimator` adds is deliberately *not* counted here —
    /// `reserve` never claimed it).
    reserved_bytes: usize,
}

impl PartitionBatch {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            records: Vec::new(),
            waiters: Vec::new(),
            created_at: now,
            size_estimator: SizeEstimator::new(),
            full: false,
            reserved_bytes: 0,
        }
    }

    pub(crate) fn append(
        &mut self,
        payload: RecordPayload,
        max_batch_bytes: usize,
    ) -> AppendOutcome {
        // Estimate before the payload's `Bytes` are moved into `record` —
        // the running batch estimate is uncompressed bytes, same unit the
        // accumulator's `max_record_size` guard uses.
        let record_size =
            estimate_record_size(payload.key.as_ref(), payload.value.as_ref(), &payload.headers);
        // Same number `Producer::send` reserved against `buffer_memory`
        // for this record; tracked so the accumulator can release it on
        // drain.
        self.reserved_bytes += record_size;

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

        self.size_estimator.add(record_size);
        let is_full = self.size_estimator.is_full(max_batch_bytes);
        self.full = is_full;

        let (tx, rx) = oneshot::channel();
        self.records.push(record);
        self.waiters.push(tx);

        AppendOutcome { rx, is_full }
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    /// True once the size estimator has crossed `max_batch_bytes` on a prior
    /// `append`. Lets the accumulator flush a full batch on the next drain
    /// without re-running the estimator.
    pub(crate) fn is_full(&self) -> bool {
        self.full
    }

    pub(crate) fn age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(self.created_at)
    }

    /// Total `buffer_memory` budget reserved for this batch's records —
    /// the amount the accumulator releases when it drains the batch.
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    /// Encode the batch into a single RecordBatch v2 frame, suitable for
    /// `ProduceRequest.topic_data[_].partition_data[_].records`.
    ///
    /// `max_request_size` caps the *encoded* frame: the accumulator's
    /// `batch_size_bytes` estimate is uncompressed and approximate, so a
    /// frame can still land over the cap (compression amplification on
    /// incompressible data, or estimator undershoot). Such a frame would
    /// be rejected by the broker as `MESSAGE_TOO_LARGE`; surface it here
    /// as `Error::Broker { MessageTooLarge }` instead.
    ///
    /// On *any* failure the batch's `waiters` are handed back inside the
    /// [`FreezeError`] rather than dropped: the drain path is the only
    /// thing still holding them, so dropping the senders here would
    /// strand every `Producer::send` for this batch on a misleading
    /// `producer closed` error (F4).
    pub(crate) fn freeze(
        mut self,
        state: BatchProducerState,
        compression: Compression,
        max_request_size: usize,
    ) -> std::result::Result<FrozenBatch, FreezeError> {
        // Force the v2-batch invariants uniformly across every record. The
        // per-record `sequence` runs `base_sequence + i`; the encoder reads
        // the first record's `sequence` as the batch-level `base_sequence`,
        // and verifies `(offset - sequence) == constant` for every record
        // in the batch (see kafka-protocol-0.17.0/src/records.rs:287).
        for (i, r) in self.records.iter_mut().enumerate() {
            r.producer_id = state.producer_id;
            r.producer_epoch = state.producer_epoch;
            r.partition_leader_epoch = state.partition_leader_epoch;
            r.transactional = state.transactional;
            r.control = state.control;
            r.sequence = state.base_sequence.wrapping_add(i as i32);
        }

        let options = RecordEncodeOptions {
            version: 2,
            compression,
        };
        let mut buf = BytesMut::new();
        if let Err(e) = RecordBatchEncoder::encode(&mut buf, self.records.iter(), &options) {
            return Err(FreezeError {
                error: Error::Protocol(format!("record batch encode: {e}")),
                waiters: self.waiters,
            });
        }
        let encoded = buf.freeze();

        // Post-encode guard: check the real (post-compression) frame, not
        // the uncompressed `append`-time estimate. An over-cap frame is
        // refused here rather than handed to the broker.
        if encoded.len() > max_request_size {
            return Err(FreezeError {
                error: Error::Broker {
                    error: ResponseError::MessageTooLarge,
                },
                waiters: self.waiters,
            });
        }

        Ok(FrozenBatch {
            encoded,
            waiters: self.waiters,
            record_count: self.records.len(),
            state,
        })
    }
}

/// Estimate the uncompressed v2 wire footprint of a single record: a
/// fixed per-record overhead plus key, value, and header bytes. This is
/// the unit the accumulator's batch-size estimator sums, the unit
/// `Accumulator::append` checks against `max_record_size`, and the unit
/// `Producer::send` reserves against `buffer_memory` — so it is taken on
/// both the `ProducerRecord` (in `send`, before a `RecordPayload`
/// exists) and the `RecordPayload` (in `append`). It is an estimate, not
/// the exact encoded length — varint field widths and compression are
/// not modelled — so it is only ever compared against approximate caps.
pub(crate) fn estimate_record_size(
    key: Option<&Bytes>,
    value: Option<&Bytes>,
    headers: &IndexMap<StrBytes, Option<Bytes>>,
) -> usize {
    const PER_RECORD_OVERHEAD: usize = 24;
    const PER_HEADER_OVERHEAD: usize = 8;

    let mut size = PER_RECORD_OVERHEAD;
    if let Some(k) = key {
        size += k.len();
    }
    if let Some(v) = value {
        size += v.len();
    }
    for (hk, hv) in headers {
        size += PER_HEADER_OVERHEAD + hk.len();
        if let Some(v) = hv.as_ref() {
            size += v.len();
        }
    }
    size
}

/// Running uncompressed-size estimate for a v2 `PartitionBatch`, seeded
/// with the fixed batch-header overhead.
struct SizeEstimator(usize);
impl SizeEstimator {
    fn new() -> Self {
        // Header
        const BATCH_HEADER_OVERHEAD: usize = 61;

        Self(BATCH_HEADER_OVERHEAD)
    }

    /// Add one record's estimated size (see [`estimate_record_size`]).
    fn add(&mut self, record_size: usize) {
        self.0 += record_size;
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
                assert!(
                    batch.is_full(),
                    "is_full() query must agree with append outcome"
                );
            }
            last_was_full = outcome.is_full;
        }
        assert!(seen_full, "threshold never crossed");
        assert!(last_was_full);
        assert!(batch.is_full());
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
            .freeze(
                BatchProducerState::non_idempotent(),
                Compression::None,
                usize::MAX,
            )
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
            .freeze(
                BatchProducerState::non_idempotent(),
                Compression::None,
                usize::MAX,
            )
            .expect("encode plain");
        let frozen_gz = gzipped
            .freeze(
                BatchProducerState::non_idempotent(),
                Compression::Gzip,
                usize::MAX,
            )
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

    #[test]
    fn freeze_rejects_batch_exceeding_max_request_size() {
        // Variant 1 — Compression::None: a single 4 KiB record encodes
        // to a frame well over a 1 KiB cap, so freeze must reject it as
        // MessageTooLarge rather than hand an oversized frame to the
        // broker.
        let mut over = PartitionBatch::new(Instant::now());
        let _ = over.append(big_value_payload(4096), usize::MAX);
        match over.freeze(
            BatchProducerState::non_idempotent(),
            Compression::None,
            1024,
        ) {
            Err(FreezeError {
                error: Error::Broker { error },
                ..
            }) => assert_eq!(error, ResponseError::MessageTooLarge),
            Err(FreezeError { error, .. }) => {
                panic!("expected Broker(MessageTooLarge), got {error}")
            }
            Ok(_) => panic!("over-cap uncompressed frame must be rejected"),
        }

        // Variant 2 — Compression::Gzip: the same highly repetitive
        // payload, four copies of it, compresses far below the 1 KiB
        // cap. freeze succeeds — proving the guard measures the real
        // post-compression frame, not the uncompressed append estimate.
        let mut compressible = PartitionBatch::new(Instant::now());
        for _ in 0..4 {
            let _ = compressible.append(big_value_payload(4096), usize::MAX);
        }
        let frozen = compressible
            .freeze(
                BatchProducerState::non_idempotent(),
                Compression::Gzip,
                1024,
            )
            .expect("gzip of a repetitive payload fits under the cap");
        assert!(
            frozen.encoded.len() <= 1024,
            "gzip frame {} B should be under the 1 KiB cap",
            frozen.encoded.len(),
        );
    }

    #[tokio::test]
    async fn waiters_survive_freeze() {
        let mut batch = PartitionBatch::new(Instant::now());
        let out1 = batch.append(sample_payload(None, Some(b"a")), usize::MAX);
        let out2 = batch.append(sample_payload(None, Some(b"b")), usize::MAX);

        let frozen = batch
            .freeze(
                BatchProducerState::non_idempotent(),
                Compression::None,
                usize::MAX,
            )
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
    fn freeze_idempotent_sets_producer_id_epoch_and_sequence() {
        let mut batch = PartitionBatch::new(Instant::now());
        let _ = batch.append(sample_payload(None, Some(b"a")), usize::MAX);
        let _ = batch.append(sample_payload(None, Some(b"b")), usize::MAX);
        let _ = batch.append(sample_payload(None, Some(b"c")), usize::MAX);

        let frozen = batch
            .freeze(
                BatchProducerState::idempotent(123, 7, 100),
                Compression::None,
                usize::MAX,
            )
            .expect("encode");
        assert_eq!(frozen.record_count, 3);

        let mut cursor = Cursor::new(frozen.encoded.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode");
        assert_eq!(decoded.records.len(), 3);
        for (i, r) in decoded.records.iter().enumerate() {
            assert_eq!(r.producer_id, 123, "record {i} producer_id");
            assert_eq!(r.producer_epoch, 7, "record {i} producer_epoch");
            assert_eq!(r.sequence, 100 + i as i32, "record {i} sequence");
            assert_eq!(r.offset, i as i64, "record {i} offset");
        }
    }
}
