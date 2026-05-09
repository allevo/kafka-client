use std::io::Cursor;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use kafka_protocol::records::{Compression, NO_TIMESTAMP, RecordBatchDecoder};

use crate::PartitionId;
use crate::consumer::ConsumerRecord;
use crate::error::{Error, Result};

/// Decode the per-partition `records` blob returned by a Fetch response into
/// a flat `Vec<ConsumerRecord>`.
///
/// The broker may pack one or more record batches into a single Fetch response
/// for a partition; loop until the cursor is drained. Control records (txn
/// markers) are filtered out — they are never user data.
pub(super) fn decode_partition_records(
    topic: &TopicName,
    partition: PartitionId,
    records: Option<Bytes>,
) -> Result<Vec<ConsumerRecord>> {
    let Some(records) = records else {
        return Ok(Vec::new());
    };

    let mut out = Vec::new();
    let mut cursor = Cursor::new(records.as_ref());
    while (cursor.position() as usize) < records.len() {
        let batch = RecordBatchDecoder::decode(&mut cursor)
            .map_err(|e| Error::Protocol(format!("record batch decode failed: {e}")))?;

        // Slice 1 only supports uncompressed and gzip on the read path.
        // Snappy/Lz4/Zstd would require pulling in extra decoders; surface
        // them as a protocol error rather than silently dropping data.
        match batch.compression {
            Compression::None | Compression::Gzip => {}
            other => {
                return Err(Error::Protocol(format!(
                    "unsupported compression {other:?} on fetch response",
                )));
            }
        }

        for record in batch.records {
            if record.control {
                continue;
            }
            out.push(ConsumerRecord {
                topic: topic.clone(),
                partition,
                offset: record.offset,
                timestamp: if record.timestamp == NO_TIMESTAMP {
                    None
                } else {
                    Some(record.timestamp)
                },
                key: record.key,
                value: record.value,
                headers: record.headers,
            });
        }
    }
    Ok(out)
}
