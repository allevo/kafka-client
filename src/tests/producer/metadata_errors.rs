//! Producer behaviour when `Client::topic_metadata` returns a cached
//! entry whose `error` field is set. Companion to
//! `tests::topic_metadata::refresh_topics_surfaces_topic_level_errors`,
//! which only exercises the cache itself — these tests pin the
//! producer-side recovery path.

use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::ResponseError;
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

/// A cached `UnknownTopicOrPartition` is `RetryAction::RefreshMetadata`
/// — the producer must refresh and re-read the cache rather than fail
/// with the stale error. After we create the topic between the prefetch
/// and the send, the second metadata fetch sees the new entry and the
/// record round-trips.
#[tokio::test]
#[tracing_test::traced_test]
async fn cached_unknown_topic_refreshes_then_succeeds() {
    let client = connect().await;
    let name = "producer-cached-unknown-then-create";
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // Seed the cache with `Some(UnknownTopicOrPartition)`. Mirrors the
    // setup in `refresh_topics_surfaces_topic_level_errors`.
    client
        .refresh_topics(std::slice::from_ref(&topic))
        .await
        .unwrap();
    let cached = client.topic_metadata(&topic).expect("errored entry cached");
    assert!(matches!(
        cached.error,
        Some(ResponseError::UnknownTopicOrPartition)
    ));

    // Now make the topic exist for real.
    create_topic(&client, name, 1).await;

    let producer = crate::Producer::new(
        client,
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    )
    .expect("producer config");
    let fut = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"hello")),
            None,
        )
        .await
        .expect("enqueue");
    let meta = fut.await.expect("ack");

    assert_eq!(meta.partition, PartitionId(0));
    assert_eq!(meta.topic, topic);

    producer.close().await.unwrap();
}

/// A topic name the broker considers invalid (Kafka rejects names
/// containing characters outside `[a-zA-Z0-9._-]`) returns
/// `INVALID_TOPIC_EXCEPTION`, which classifies as `Fatal` and **must
/// not** trigger the refresh loop. The producer must surface
/// `Error::Broker { error: InvalidTopicException }` immediately —
/// well under `max_block`.
///
/// This pins the fail-fast branch of `resolve_topic_metadata`: a
/// non-`RefreshMetadata` cached error short-circuits the loop. The
/// timeout branch (loop runs out the budget) is harder to exercise
/// against testcontainers' Kafka because `auto.create.topics.enable`
/// is on by default and a sustained loop ends up auto-creating the
/// topic. The control-flow contract for that branch — "stop after
/// `max_block` elapses" — is structurally evident in the loop body.
#[tokio::test]
#[tracing_test::traced_test]
async fn invalid_topic_name_fails_fast() {
    let client = connect().await;
    // Colons aren't allowed in Kafka topic names — broker returns
    // INVALID_TOPIC_EXCEPTION on the metadata round-trip.
    let name = "invalid:producer:topic";
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // Generous max_block: if the loop misclassified InvalidTopicException
    // as refreshable, the test would block for the full budget instead
    // of failing fast.
    let max_block = Duration::from_secs(10);
    let producer = crate::Producer::new(
        client,
        crate::ProducerConfig::default()
            .with_linger(Duration::from_millis(0))
            .with_max_block(max_block),
    )
    .expect("producer config");

    let started = std::time::Instant::now();
    let result = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"hello")),
            None,
        )
        .await;
    let elapsed = started.elapsed();

    match result {
        Ok(_) => panic!("send to invalid topic name must fail at the metadata step"),
        Err(Error::Broker {
            error: ResponseError::InvalidTopicException,
        }) => {}
        Err(other) => panic!("expected Broker(InvalidTopicException), got: {other:?}"),
    }

    // Fail-fast: must complete in well under `max_block`. A loose 2s
    // ceiling absorbs broker round-trip jitter without letting a
    // misclassification slip through.
    assert!(
        elapsed < Duration::from_secs(2),
        "fail-fast path took too long: {elapsed:?}",
    );

    producer.close().await.unwrap();
}
