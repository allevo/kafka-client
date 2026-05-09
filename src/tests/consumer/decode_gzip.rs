use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;

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
async fn gzip_round_trip() {
    let client = connect().await;
    let name = "consumer-decode-gzip";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // Gzip on the produce side exercises the consumer's gzip decode branch
    // in `decode_partition_records` end-to-end. Linger=0 keeps produce
    // latency predictable so the consumer collect deadline isn't dominated
    // by batch wakeups.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_compression(Compression::Gzip)
            .with_linger(Duration::from_millis(0)),
    );

    const N: usize = 8;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        // Repeating payload compresses well — sanity that gzip is actually
        // engaged on the wire and not silently falling back to None.
        let payload = Bytes::from(format!("gzip-payload-{i}-{}", "x".repeat(64)));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }
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
        assert_eq!(rec.offset, i as i64);
        let expected = format!("gzip-payload-{i}-{}", "x".repeat(64));
        assert_eq!(
            rec.value.as_deref(),
            Some(expected.as_bytes()),
            "payload mismatch at offset {i}"
        );
    }

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}
