use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use crate::tests::helpers;
use crate::tests::helpers::{create_topic, topic_partition};

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

#[tokio::test]
#[tracing_test::traced_test]
async fn close_joins_fetcher_when_backpressured() {
    let client = connect().await;
    let name = "consumer-close-clean";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // linger = 0 (floored to the sender's 1ms tick) keeps the produce
    // phase below the close deadline below; same shape as the round_trip
    // / earliest_latest tests.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    )
    .expect("producer config");

    // Produce many more records than the consumer's `channel_capacity`
    // so the fetcher fills the internal record channel, then parks on
    // `record_tx.reserve()` while the slow consumer below sleeps. The
    // close path under test is "shutdown preempts a parked fetcher",
    // not "shutdown after the channel drained".
    const N: usize = 200;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        let payload = Bytes::from(format!("close-clean-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }
    for fut in futures {
        fut.await.expect("ack");
    }

    // Tiny `channel_capacity` makes the fetcher fill the buffer almost
    // immediately, then park on `record_tx.reserve()` until the slow
    // consumer drains a slot. By the time we call `close()` the fetcher
    // is parked mid-emission; close must preempt that wait via the
    // biased `select!` in `fetcher_main`.
    let mut consumer = crate::Consumer::new(
        client.clone(),
        crate::ConsumerConfig::default().with_channel_capacity(2),
    );
    let tp = topic_partition(name, 0);
    consumer.assign(vec![tp]).await.expect("assign");

    // Deliberately slow consumer: pull a handful of records with 100ms
    // sleeps in between so the fetcher backlog grows past the channel
    // capacity and parks the fetcher on `record_tx.reserve()`.
    for i in 0..5 {
        let rec = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
            .await
            .unwrap_or_else(|_| panic!("recv timed out at index {i}"))
            .expect("stream ended early")
            .expect("consumer error");
        assert_eq!(rec.topic, topic);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // close() must join the fetcher cleanly even though it is parked on
    // `record_tx.reserve()` with hundreds of records still upstream.
    // Bounded by a generous timeout: a hang here means shutdown did not
    // preempt the parked fetcher, which is the bug this test guards.
    //
    // The successful join is itself proof of the Stream's `Ready(None)`
    // contract: `handle.await` completes only after the fetcher task
    // returns and drops its `record_tx` clone, which closes the channel.
    // Any subsequent `poll_recv` on the receiver would observe
    // `Ready(None)` once the buffered records drain — but the receiver
    // lives inside the consumer that `close(self)` just consumed, so it
    // cannot be polled here directly.
    tokio::time::timeout(Duration::from_secs(10), consumer.close())
        .await
        .expect("close timed out — fetcher did not exit on shutdown")
        .expect("close returned an error");

    producer.close().await.unwrap();
}
