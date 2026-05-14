use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use crate::client::PartitionId;
use crate::error::Error;
use crate::tests::helpers;
use crate::tests::helpers::create_topic;

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

/// A record whose estimated wire size exceeds `max_record_size` must be
/// rejected synchronously by `Producer::send`: the call returns
/// `Err(RecordTooLarge)` rather than handing back a `SendFuture`, so no
/// waiter is ever allocated and the accumulator is never mutated. The
/// guard is per-record — an in-cap record on the same producer still
/// sends and acks normally afterwards, proving the oversized record did
/// not wedge the producer or its sender task.
#[tokio::test]
async fn record_too_large_returns_synchronous_error() {
    let client = connect().await;
    let name = "producer-record-too-large";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default().with_max_record_size(1024),
    )
    .expect("producer config");

    // ~64 KiB payload — far over the 1 KiB max_record_size.
    let oversized = Bytes::from(vec![b'x'; 64 * 1024]);
    let result = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), oversized)
                .with_partition(PartitionId(0)),
            None,
        )
        .await;
    match result {
        Err(Error::RecordTooLarge { size, limit }) => {
            assert!(size > 1024, "reported size {size} must exceed the limit");
            assert_eq!(limit, 1024);
        }
        Err(other) => panic!("expected RecordTooLarge, got {other}"),
        Ok(_) => panic!("oversized record must be rejected synchronously"),
    }

    // The producer is still healthy: an in-cap record enqueues and acks.
    let fut = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"small"))
                .with_partition(PartitionId(0)),
            None,
        )
        .await
        .expect("an in-cap record still enqueues");
    let meta = fut.await.expect("an in-cap record still acks");
    assert_eq!(meta.offset, 0, "the in-cap record is the first on the partition");

    producer.close().await.unwrap();
}
