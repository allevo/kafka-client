//! Tests for the topic-routing metadata cache added in
//! `PRODUCER_METADATA.md`. Covers the bootstrap-populate path, the full
//! replace in `refresh_metadata`, the partial merge in `refresh_topics`,
//! and the topic-level error surface.

use kafka_protocol::ResponseError;
use kafka_protocol::messages::TopicName;
use kafka_protocol::protocol::StrBytes;

use super::helpers;
use super::helpers::{create_topic, delete_topic};

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

#[tokio::test]
#[tracing_test::traced_test]
async fn topic_metadata_populated_at_bootstrap() {
    let seed = connect().await;
    let name = "t-md-populated";
    create_topic(&seed, name, 1).await;

    // Fresh connect so the bootstrap path is what populates the cache.
    let client = connect().await;
    let meta = client
        .topic_metadata(&TopicName::from(StrBytes::from_static_str(name)))
        .unwrap_or_else(|| panic!("topic '{name}' not cached at bootstrap"));
    assert_eq!(meta.partitions.len(), 1, "expected single partition");
    assert!(
        meta.error.is_none(),
        "unexpected topic-level error: {meta:?}"
    );

    let part = crate::client::find_partition(&meta.partitions, crate::client::PartitionId(0))
        .expect("partition 0 must be present for a single-partition topic");
    // The leader must be a broker id known to the snapshot. We can't read
    // the snapshot directly, but `client.broker(id)` succeeding implies
    // membership.
    client.broker(part.leader).await.unwrap();
}

#[tokio::test]
#[tracing_test::traced_test]
async fn refresh_metadata_replaces_fully() {
    let client = connect().await;
    let a = "t-md-replace-a";
    let b = "t-md-replace-b";
    create_topic(&client, a, 1).await;
    create_topic(&client, b, 1).await;

    // Force-refresh so both topics are cached.
    client.refresh_metadata().await.unwrap();
    assert!(
        client
            .topic_metadata(&TopicName::from(StrBytes::from_static_str(a)))
            .is_some(),
        "'{a}' missing after initial refresh"
    );
    assert!(
        client
            .topic_metadata(&TopicName::from(StrBytes::from_static_str(b)))
            .is_some(),
        "'{b}' missing after initial refresh"
    );

    delete_topic(&client, a).await;
    client.refresh_metadata().await.unwrap();

    assert!(
        client
            .topic_metadata(&TopicName::from(StrBytes::from_static_str(a)))
            .is_none(),
        "'{a}' still cached after delete + full refresh"
    );
    assert!(
        client
            .topic_metadata(&TopicName::from(StrBytes::from_static_str(b)))
            .is_some(),
        "'{b}' evicted by full refresh — should have been preserved"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn refresh_topics_merges_without_evicting() {
    let client = connect().await;
    let keep = "t-md-keep";
    let new = "t-md-new";
    create_topic(&client, keep, 1).await;

    // Seed `keep` into the cache via bootstrap-equivalent full refresh.
    client.refresh_metadata().await.unwrap();
    assert!(
        client
            .topic_metadata(&TopicName::from(StrBytes::from_static_str(keep)))
            .is_some(),
        "'{keep}' missing before merge"
    );

    create_topic(&client, new, 1).await;
    client
        .refresh_topics(&[TopicName::from(StrBytes::from_static_str(new))])
        .await
        .unwrap();

    assert!(
        client
            .topic_metadata(&TopicName::from(StrBytes::from_static_str(keep)))
            .is_some(),
        "'{keep}' evicted by partial refresh — merge regressed to clobber"
    );
    assert!(
        client
            .topic_metadata(&TopicName::from(StrBytes::from_static_str(new)))
            .is_some(),
        "'{new}' missing after merge"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn refresh_topics_surfaces_topic_level_errors() {
    let client = connect().await;
    let missing = "t-md-does-not-exist";

    client
        .refresh_topics(&[TopicName::from(StrBytes::from_static_str(missing))])
        .await
        .unwrap();

    let meta = client
        .topic_metadata(&TopicName::from(StrBytes::from_static_str(missing)))
        .unwrap_or_else(|| panic!("errored topic '{missing}' must be cached, not dropped"));
    assert!(
        matches!(meta.error, Some(ResponseError::UnknownTopicOrPartition)),
        "expected UnknownTopicOrPartition, got: {:?}",
        meta.error
    );
    assert!(
        meta.partitions.is_empty(),
        "errored topic should have no partitions"
    );
}

#[tokio::test]
#[tracing_test::traced_test]
async fn refresh_topics_empty_slice_is_noop() {
    let client = connect().await;
    // No panic, no network call required (we can't observe the lack of a
    // call directly, but a zero-retry send on an empty topics list would
    // produce a visibly different error shape than Ok).
    client.refresh_topics(&[]).await.unwrap();
}
