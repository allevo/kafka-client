use std::time::Duration;

use bytes::Bytes;
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

#[tokio::test]
#[tracing_test::traced_test]
async fn single_record_round_trip() {
    let client = connect().await;
    let name = "producer-single-record";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // linger = 0 floors to the sender's 1ms minimum interval, so the
    // first tick fires almost immediately and we don't depend on the
    // size-full / wakeup path to ship a single record.
    let producer = crate::Producer::new(
        client,
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    );

    let fut = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"hello")),
            None,
        )
        .await
        .expect("enqueue");
    let meta = fut.await.expect("ack");

    // First send to a freshly created single-partition topic always
    // lands at offset 0 / partition 0. Anything else would mean the
    // shared broker was reused without `name` being unique.
    assert_eq!(meta.partition, PartitionId(0));
    assert_eq!(meta.offset, 0);
    assert_eq!(meta.topic, topic);

    producer.close().await.unwrap();
}
