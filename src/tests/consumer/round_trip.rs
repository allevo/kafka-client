use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use crate::client::PartitionId;
use crate::tests::helpers;
use crate::tests::helpers::{collect_n, create_topic, topic_partition};

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

#[tokio::test]
#[tracing_test::traced_test]
async fn round_trip_single_partition() {
    let client = connect().await;
    let name = "consumer-round-trip";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // linger = 0 floors to the sender's 1ms minimum interval so each
    // record ships on the next tick — keeps the produce phase below
    // the consumer collect deadline without relying on size-based
    // batch wakeups.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    )
    .expect("producer config");

    const N: usize = 8;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        let payload = Bytes::from(format!("round-trip-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }
    // Block until every record is acked: the consumer's start-offset
    // resolution (`Earliest`) reads the high-water mark at assign time,
    // and we want all N records visible before that snapshot.
    for fut in futures {
        fut.await.expect("ack");
    }

    let mut consumer = crate::Consumer::new(client.clone(), crate::ConsumerConfig::default());
    let tp = topic_partition(name, 0);
    consumer.assign(vec![tp]).await.expect("assign");

    let records = collect_n(&mut consumer, N, Duration::from_secs(30)).await;

    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.topic, topic);
        assert_eq!(rec.partition, PartitionId(0));
        // Fresh single-partition topic + acks-blocked produce above
        // means offsets are dense `0..N` with no gaps.
        assert_eq!(rec.offset, i as i64);
        let expected = format!("round-trip-{i}");
        assert_eq!(
            rec.value.as_deref(),
            Some(expected.as_bytes()),
            "payload mismatch at offset {i}"
        );
    }

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}
