use std::io::Cursor;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{ApiKey, BrokerId, FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;

use crate::client::PartitionId;
use crate::tests::helpers;
use crate::tests::helpers::create_topic;

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

/// A tight produce burst to a single partition must rotate the
/// accumulator's per-partition batch queue: with `batch_size_bytes`
/// well below the total payload, the 200 records cannot coalesce into
/// one wire frame. Re-fetching the raw log proves the records landed as
/// many distinct v2 record-batch frames with a single contiguous offset
/// run — i.e. rotation produced correctly sized batches and the
/// non-idempotent send path shipped every one of them (no silent drop
/// from duplicate partition indices in the `ProduceRequest`).
#[tokio::test]
#[tracing_test::traced_test]
async fn single_partition_burst_produces_multiple_wire_batches() {
    let client = connect().await;
    let name = "producer-burst-rotation";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // 4 KiB batch cap vs. ~500 B records: a batch fills after ~8
    // records, so 200 records must rotate into ~25 batches.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_batch_size_bytes(4 * 1024)
            .with_linger(Duration::from_millis(5)),
    )
    .expect("producer config");

    const N: usize = 200;
    const PAYLOAD_LEN: usize = 500;
    let mut futures = Vec::with_capacity(N);
    for _ in 0..N {
        let payload = Bytes::from(vec![b'x'; PAYLOAD_LEN]);
        let fut = producer
            .send(
                crate::ProducerRecord::new(topic.clone(), payload).with_partition(PartitionId(0)),
                None,
            )
            .await
            .expect("enqueue");
        futures.push(fut);
    }
    for fut in futures {
        fut.await.expect("ack");
    }

    let leader = leader_for(&client, &topic).await;
    let batches = fetch_decoded_batches(&client, leader, &topic).await;

    // Rotation must have produced many distinct wire frames — a single
    // coalesced batch would mean rotation never fired.
    assert!(
        batches.len() >= 20,
        "expected >= 20 rotated record-batch frames, got {}",
        batches.len(),
    );

    // Every record is present exactly once, offsets run 0..N with no
    // gap or duplicate — proof that no rotated batch was silently
    // dropped or double-counted on the wire.
    let mut expected_offset = 0i64;
    let mut count = 0usize;
    for batch in &batches {
        for r in &batch.records {
            assert_eq!(r.offset, expected_offset, "non-monotonic offset at {count}");
            expected_offset += 1;
            count += 1;
        }
    }
    assert_eq!(
        count, N,
        "expected exactly {N} records on disk, found {count}"
    );

    producer.close().await.unwrap();
}

async fn leader_for(client: &crate::Client, topic: &TopicName) -> BrokerId {
    let meta = client
        .topic_metadata(topic)
        .expect("topic metadata cached after create_topic");
    meta.partitions
        .iter()
        .find(|(p, _)| *p == PartitionId(0))
        .map(|(_, m)| m.leader)
        .expect("partition 0 must exist")
}

async fn fetch_decoded_batches(
    client: &crate::Client,
    leader: BrokerId,
    topic: &TopicName,
) -> Vec<kafka_protocol::records::RecordSet> {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(0)
        .with_max_bytes(8 * 1024 * 1024)
        .with_isolation_level(0)
        .with_session_id(0)
        .with_session_epoch(-1)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic.clone())
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_current_leader_epoch(-1)
                        .with_fetch_offset(0)
                        .with_last_fetched_epoch(-1)
                        .with_log_start_offset(-1)
                        .with_partition_max_bytes(8 * 1024 * 1024),
                ]),
        ]);

    let resp: FetchResponse = client
        .send(
            crate::NodeTarget::Broker(leader),
            ApiKey::Fetch,
            12,
            request,
            &crate::CallOptions::default(),
        )
        .await
        .expect("fetch must succeed");

    let mut out = Vec::new();
    for tr in &resp.responses {
        for pr in &tr.partitions {
            let Some(records) = pr.records.as_ref() else {
                continue;
            };
            let mut cursor = Cursor::new(records.as_ref());
            while (cursor.position() as usize) < records.len() {
                match RecordBatchDecoder::decode(&mut cursor) {
                    Ok(batch) => out.push(batch),
                    Err(e) => {
                        tracing::error!(error = ?e, "record batch decode failed");
                        break;
                    }
                }
            }
        }
    }
    out
}
