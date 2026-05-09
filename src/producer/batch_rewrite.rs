//! In-place rewrite of v2 RecordBatch headers.
//!
//! Mirrors Java's `MemoryRecordsBuilder.reopenAndRewriteProducerState`:
//! after an epoch bump (KIP-98 `OutOfOrderSequence` recovery), each
//! paused in-flight batch needs a fresh `(producer_id, producer_epoch,
//! base_sequence)` triple stamped into its already-encoded bytes. The
//! records section is left untouched — compression wraps only bytes
//! `RECORDS_OFFSET..` of the v2 frame, and per-record `sequence` is
//! derived broker-side from `base_sequence + offset_delta`, so no
//! per-record byte mutation is needed.
//!
//! Byte offsets match Apache Kafka `DefaultRecordBatch` (verified
//! against `org/apache/kafka/common/record/DefaultRecordBatch.java`):
//!
//! | Field            | Offset | Bytes | Type      |
//! |------------------|-------:|------:|-----------|
//! | magic            |     16 |     1 | u8        |
//! | crc32c           |     17 |     4 | u32 BE    |
//! | attributes       |     21 |     2 | i16 BE    |
//! | producer_id      |     43 |     8 | i64 BE    |
//! | producer_epoch   |     51 |     2 | i16 BE    |
//! | base_sequence    |     53 |     4 | i32 BE    |
//! | records section  |     61 |     — | varies    |
//!
//! CRC inputs are bytes `ATTRIBUTES_OFFSET..` (the entire frame minus
//! the leading magic + CRC and the offset/length prefix that precedes
//! every record-batch in a `MemoryRecords` blob). The records section
//! is included in the CRC even when compressed — the compressed bytes
//! are stable across a header rewrite, so only the 14 mutated header
//! bytes contribute new CRC input.
//!
//! # Caller invariants
//!
//! The caller must hold the only reference to the underlying buffer
//! while this function runs. Once an idempotent in-flight batch's
//! encoded `Bytes` has been cloned into a `ProduceRequest` on the
//! wire, mutating the underlying allocation would race the wire write.
//! In the bump-pass, this is naturally satisfied: the bump runs only
//! after the previous dispatch's response has settled and the wire
//! copy has been dropped.

use crate::error::{Error, Result};

const MAGIC_OFFSET: usize = 16;
const CRC_OFFSET: usize = 17;
const ATTRIBUTES_OFFSET: usize = 21;
const PRODUCER_ID_OFFSET: usize = 43;
const PRODUCER_EPOCH_OFFSET: usize = 51;
const BASE_SEQUENCE_OFFSET: usize = 53;
const RECORDS_OFFSET: usize = 61;
const MAGIC_V2: u8 = 2;

/// Rewrite the `(producer_id, producer_epoch, base_sequence)` triple in
/// `buf` in place and recompute the batch's CRC32C. Used by the
/// idempotent producer's `UnknownProducerId` head-recovery path
/// (single-batch rewrite to `base_sequence=0`) and the epoch-bump pass
/// (multi-batch rewrite under a fresh `(pid, epoch)`). See module docs
/// for caller invariants.
pub(crate) fn rewrite_producer_state(
    buf: &mut [u8],
    new_pid: i64,
    new_epoch: i16,
    new_base_seq: i32,
) -> Result<()> {
    if buf.len() < RECORDS_OFFSET {
        return Err(Error::Protocol(format!(
            "v2 record batch too short to rewrite: {} bytes (need >= {RECORDS_OFFSET})",
            buf.len(),
        )));
    }
    if buf[MAGIC_OFFSET] != MAGIC_V2 {
        return Err(Error::Protocol(format!(
            "v2 record batch rewrite: unexpected magic {} (need {MAGIC_V2})",
            buf[MAGIC_OFFSET],
        )));
    }
    buf[PRODUCER_ID_OFFSET..PRODUCER_ID_OFFSET + 8].copy_from_slice(&new_pid.to_be_bytes());
    buf[PRODUCER_EPOCH_OFFSET..PRODUCER_EPOCH_OFFSET + 2]
        .copy_from_slice(&new_epoch.to_be_bytes());
    buf[BASE_SEQUENCE_OFFSET..BASE_SEQUENCE_OFFSET + 4]
        .copy_from_slice(&new_base_seq.to_be_bytes());
    // CRC inputs span attributes through the end of the (possibly
    // compressed) records section. Java's `Crc32C.compute(buf,
    // ATTRIBUTES_OFFSET, size - ATTRIBUTES_OFFSET)` does the same.
    let crc = crc32c::crc32c(&buf[ATTRIBUTES_OFFSET..]);
    buf[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::records::{
        Compression, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record, RecordBatchDecoder,
        RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    };
    use std::io::Cursor;

    /// Build a 3-record v2 batch encoded with the given producer state
    /// and compression. Returns the encoded `Bytes`.
    fn encode_batch(
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        compression: Compression,
    ) -> Bytes {
        let records: Vec<Record> = (0..3)
            .map(|i| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id,
                producer_epoch,
                timestamp_type: TimestampType::Creation,
                offset: i,
                sequence: base_sequence.wrapping_add(i as i32),
                timestamp: 1_700_000_000_000 + i,
                key: Some(Bytes::from(format!("k{i}"))),
                value: Some(Bytes::from(format!("v{i}").repeat(64))),
                headers: IndexMap::new(),
            })
            .collect();
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .expect("encode");
        buf.freeze()
    }

    #[test]
    fn rewrite_updates_producer_id_epoch_base_sequence_and_keeps_crc_valid() {
        let original = encode_batch(42, 1, 100, Compression::None);
        let mut buf = BytesMut::from(&original[..]);
        rewrite_producer_state(&mut buf, 99, 5, 0).expect("rewrite");

        let frozen = buf.freeze();
        let mut cursor = Cursor::new(frozen.as_ref());
        // RecordBatchDecoder verifies CRC during decode (kafka-protocol
        // 0.17 records.rs:517) — a wrong CRC here would surface as a
        // decode error rather than silently passing.
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode validates CRC");
        assert_eq!(decoded.records.len(), 3);
        for (i, r) in decoded.records.iter().enumerate() {
            assert_eq!(r.producer_id, 99, "record {i} producer_id");
            assert_eq!(r.producer_epoch, 5, "record {i} producer_epoch");
            assert_eq!(r.sequence, i as i32, "record {i} sequence (base 0 + delta)");
        }
    }

    #[test]
    fn rewrite_preserves_compressed_records() {
        // Gzip wraps only bytes [61..]; the rewrite touches header
        // bytes [17, 21), [43, 51), [51, 53), [53, 57). Compression
        // is opaque to the rewrite — the round-trip proves the
        // compressed records section is byte-stable across the bump.
        let original = encode_batch(1, 0, 50, Compression::Gzip);
        let mut buf = BytesMut::from(&original[..]);
        let pre_records = buf[RECORDS_OFFSET..].to_vec();

        rewrite_producer_state(&mut buf, 7, 3, 0).expect("rewrite gzip");

        assert_eq!(
            &buf[RECORDS_OFFSET..],
            pre_records.as_slice(),
            "compressed records section must not change",
        );

        let frozen = buf.freeze();
        let mut cursor = Cursor::new(frozen.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("decode gzip");
        assert_eq!(decoded.compression, Compression::Gzip);
        assert_eq!(decoded.records.len(), 3);
        for (i, r) in decoded.records.iter().enumerate() {
            assert_eq!(r.producer_id, 7);
            assert_eq!(r.producer_epoch, 3);
            assert_eq!(r.sequence, i as i32);
        }
    }

    #[test]
    fn rewrite_rejects_short_buffer() {
        let mut buf = vec![0u8; RECORDS_OFFSET - 1];
        let err = rewrite_producer_state(&mut buf, 1, 1, 1).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    #[test]
    fn rewrite_rejects_wrong_magic() {
        let mut buf = vec![0u8; RECORDS_OFFSET];
        buf[MAGIC_OFFSET] = 1; // v1 — we only handle v2
        let err = rewrite_producer_state(&mut buf, 1, 1, 1).unwrap_err();
        match err {
            Error::Protocol(msg) => assert!(msg.contains("magic")),
            other => panic!("expected Error::Protocol, got {other:?}"),
        }
    }

    /// Sanity: the encoder we're testing against actually produces the
    /// header layout we've hardcoded the offsets for. If kafka-protocol
    /// ever changes the v2 layout, this test fails loudly instead of
    /// silently corrupting writes.
    #[test]
    fn encoder_layout_matches_hardcoded_offsets() {
        let original = encode_batch(0xDEAD_BEEF_CAFE_BABEu64 as i64, 0x1234, 0x5678_9ABC, Compression::None);
        assert!(original.len() >= RECORDS_OFFSET);
        assert_eq!(original[MAGIC_OFFSET], MAGIC_V2);
        assert_eq!(
            i64::from_be_bytes(
                original[PRODUCER_ID_OFFSET..PRODUCER_ID_OFFSET + 8]
                    .try_into()
                    .unwrap(),
            ),
            0xDEAD_BEEF_CAFE_BABEu64 as i64,
        );
        assert_eq!(
            i16::from_be_bytes(
                original[PRODUCER_EPOCH_OFFSET..PRODUCER_EPOCH_OFFSET + 2]
                    .try_into()
                    .unwrap(),
            ),
            0x1234,
        );
        assert_eq!(
            i32::from_be_bytes(
                original[BASE_SEQUENCE_OFFSET..BASE_SEQUENCE_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            ),
            0x5678_9ABC,
        );
    }

    /// A non-idempotent batch (NO_PRODUCER_ID / NO_PRODUCER_EPOCH) is
    /// still v2-formatted, and a sanity-rewrite back to the same values
    /// must round-trip without changing anything observable.
    #[test]
    fn rewrite_is_a_noop_for_same_values() {
        let original = encode_batch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH, 0, Compression::None);
        let mut buf = BytesMut::from(&original[..]);
        rewrite_producer_state(&mut buf, NO_PRODUCER_ID, NO_PRODUCER_EPOCH, 0).expect("rewrite");
        assert_eq!(buf.as_ref(), original.as_ref(), "no-op rewrite must be byte-identical");
    }
}
