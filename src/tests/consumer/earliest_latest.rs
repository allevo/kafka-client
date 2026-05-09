use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use crate::client::PartitionId;
use crate::tests::helpers;
use crate::tests::helpers::{collect_n, create_topic, topic_partition};
use crate::{ConsumerConfig, OffsetReset};

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

#[tokio::test]
#[tracing_test::traced_test]
async fn reset_earliest_reads_pre_existing() {
    let client = connect().await;
    let name = "consumer-reset-earliest";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // linger = 0 floors to the sender's 1ms minimum interval so each
    // record ships on the next tick — keeps the produce phase below
    // the consumer collect deadline without relying on size-based
    // batch wakeups.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    );

    const N: usize = 5;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        let payload = Bytes::from(format!("earliest-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }
    // Ensure all records are durably written before the consumer
    // resolves Earliest; not strictly required for correctness
    // (Earliest reads from offset 0 regardless), but keeps the test
    // deterministic on the count we expect to drain.
    for fut in futures {
        fut.await.expect("ack");
    }

    let consumer = crate::Consumer::new(
        client.clone(),
        ConsumerConfig::default().with_auto_offset_reset(OffsetReset::Earliest),
    );
    let tp = topic_partition(name, 0);
    consumer.assign(vec![tp]).await.expect("assign");

    let records = collect_n(&consumer, N, Duration::from_secs(30)).await;

    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.topic, topic);
        assert_eq!(rec.partition, PartitionId(0));
        assert_eq!(rec.offset, i as i64);
        let expected = format!("earliest-{i}");
        assert_eq!(
            rec.value.as_deref(),
            Some(expected.as_bytes()),
            "payload mismatch at offset {i}"
        );
    }

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}

#[tokio::test]
#[tracing_test::traced_test]
async fn reset_latest_skips_pre_existing() {
    let client = connect().await;
    let name = "consumer-reset-latest";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    );

    // Produce the pre-existing batch and block until every record
    // is acked: Latest resolves the high-water mark at assign time,
    // and we want the HWM to equal PRE before we build the consumer
    // so the cursor seeds at offset PRE and skips this batch.
    const PRE: usize = 5;
    let mut pre_futures = Vec::with_capacity(PRE);
    for i in 0..PRE {
        let payload = Bytes::from(format!("pre-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        pre_futures.push(fut);
    }
    for fut in pre_futures {
        fut.await.expect("ack");
    }

    let consumer = crate::Consumer::new(
        client.clone(),
        ConsumerConfig::default().with_auto_offset_reset(OffsetReset::Latest),
    );
    let tp = topic_partition(name, 0);
    consumer.assign(vec![tp]).await.expect("assign");

    // Produce the post-assign batch only after the cursor is seeded.
    // These records land at offsets `PRE..PRE+POST` and are the only
    // ones the Latest consumer should observe.
    const POST: usize = 3;
    let mut post_futures = Vec::with_capacity(POST);
    for i in 0..POST {
        let payload = Bytes::from(format!("post-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        post_futures.push(fut);
    }
    for fut in post_futures {
        fut.await.expect("ack");
    }

    let records = collect_n(&consumer, POST, Duration::from_secs(30)).await;

    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec.topic, topic);
        assert_eq!(rec.partition, PartitionId(0));
        assert_eq!(rec.offset, (PRE + i) as i64);
        let expected = format!("post-{i}");
        assert_eq!(
            rec.value.as_deref(),
            Some(expected.as_bytes()),
            "payload mismatch at offset {}",
            PRE + i
        );
    }

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}
