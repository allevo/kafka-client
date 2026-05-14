use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

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

/// Produce one record to every partition, retrying past the post-create
/// `NotLeaderOrFollower` window. `create_topic`'s propagation wait only
/// guarantees the topic is *visible* — on a single broker a partition
/// can still briefly reject produce while leadership settles, and the
/// non-idempotent producer doesn't retry that. Once each partition has
/// accepted a write the broker stays ready, so the keyless burst below
/// won't hit the race.
async fn warm_up_partitions(
    client: &crate::Client,
    producer: &crate::Producer,
    topic: &TopicName,
    partitions: i32,
) {
    for p in 0..partitions {
        let mut attempts = 0;
        loop {
            let fut = producer
                .send(
                    crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"warmup"))
                        .with_partition(PartitionId(p)),
                    None,
                )
                .await
                .expect("enqueue warmup");
            match fut.await {
                Ok(_) => break,
                Err(crate::Error::Broker {
                    error: ResponseError::NotLeaderOrFollower,
                }) if attempts < 30 => {
                    attempts += 1;
                    let _ = client.refresh_metadata().await;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                Err(e) => panic!("warmup produce to partition {p} failed: {e}"),
            }
        }
    }
}

/// Keyless records (no key, no explicit partition) take the sticky
/// branch in `partition_for`. With `batch_size_bytes` well below the
/// burst's total payload, each sticky batch fills after a handful of
/// records and `Producer::send` rotates the sticky partition (KIP-480).
/// The acked `RecordMetadata.partition` values must therefore span more
/// than one partition — before the rotation was wired up every keyless
/// record pinned to partition 0.
#[tokio::test]
#[tracing_test::traced_test]
async fn keyless_burst_rotates_across_partitions() {
    let client = connect().await;
    let name = "producer-sticky-rotation";
    create_topic(&client, name, 4).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // 4 KiB batch cap vs. ~500 B records: a sticky batch fills after ~8
    // records, so 200 keyless records force ~25 rotations across the 4
    // partitions.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_batch_size_bytes(4 * 1024)
            .with_linger(Duration::from_millis(5)),
    )
    .expect("producer config");

    // Explicit-partition writes — they take the explicit branch in
    // `partition_for`, so this leaves the sticky state untouched at its
    // initial partition 0 for the burst below.
    warm_up_partitions(&client, &producer, &topic, 4).await;

    const N: usize = 200;
    const PAYLOAD_LEN: usize = 500;
    let mut futures = Vec::with_capacity(N);
    for _ in 0..N {
        let payload = Bytes::from(vec![b'x'; PAYLOAD_LEN]);
        // No key and no explicit partition — the sticky path.
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }

    let mut partitions: HashSet<PartitionId> = HashSet::new();
    let mut count = 0usize;
    for fut in futures {
        let meta = fut.await.expect("ack");
        partitions.insert(meta.partition);
        count += 1;
    }

    assert_eq!(count, N, "every record must be acked");
    // A keyless burst that never rotated would land entirely on a single
    // partition; spanning more than one is proof rotation fired.
    assert!(
        partitions.len() >= 2,
        "keyless burst must rotate across >1 partition, landed only on {partitions:?}",
    );

    producer.close().await.unwrap();
}
