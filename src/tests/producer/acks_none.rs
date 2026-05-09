use std::io::Cursor;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse, MetadataResponse, TopicName,
};
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

#[tokio::test]
#[tracing_test::traced_test]
async fn acks_none_round_trip() {
    let client = connect().await;
    let name = "producer-acks-none";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // `create_topic`'s `wait_for_topic_visible` only proves that the
    // broker's `MetadataCache` has replayed the topic-creation record;
    // the leader's `ReplicaManager` may still be initialising the
    // partition's `UnifiedLog`. A `Produce(acks=0)` arriving in that
    // window hits `getPartitionOrException` →
    // `UnknownTopicOrPartitionException`, and because acks=0 admits no
    // response, `KafkaApis.handleProduceRequest` reacts to
    // `errorInResponse` by silently closing the socket — the records
    // are dropped and a downstream Fetch returns nothing. Refresh the
    // client's metadata so `leader_for` resolves the partition leader,
    // then drive `Fetch(max_wait_ms=0)` against that leader until it
    // reports `error_code == 0`: same `getPartitionOrException` path
    // Produce uses, so a green Fetch proves Produce will be accepted.
    client
        .refresh_topics(&[topic.clone()])
        .await
        .expect("topic metadata must refresh");
    let leader = leader_for(&client, &topic).await;
    wait_for_partition_ready(&client, leader, &topic).await;

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_acks(crate::Acks::None)
            .with_linger(Duration::from_millis(0)),
    );

    const N: usize = 5;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        let payload = Bytes::from(format!("acks-none-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }

    for fut in futures {
        let meta = fut.await.expect("ack");
        // Java surfaces -1 in fire-and-forget mode: the broker won't
        // reply, so the client cannot know the assigned offset.
        assert_eq!(meta.offset, -1);
        assert_eq!(meta.topic, topic);
        assert_eq!(meta.partition, PartitionId(0));
        assert!(meta.timestamp.is_none());
    }

    // Kafka's `SocketServer` mutes the per-connection channel for the
    // duration of a request and only reads the next one after the
    // current handler finishes (a `NoOpResponse` for acks=0). So by
    // the time the broker reads this Fetch off the socket the
    // preceding `Produce` has already appended every record to the
    // log. Combined with the readiness probe above proving the
    // partition is online, a single Fetch must see all N records — no
    // wall-clock polling required.
    let total = fetch_record_count(&client, leader, &topic).await;
    assert_eq!(total, N, "expected {N} records persisted, found {total}");

    // Connection-health probe: a follow-up Metadata request would fail
    // with mis-correlation if the OneWay path had registered a stale
    // entry in `in_flight`.
    let _meta: MetadataResponse = client
        .send(
            crate::NodeTarget::Broker(leader),
            ApiKey::Metadata,
            1,
            kafka_protocol::messages::MetadataRequest::default().with_topics(None),
            &crate::CallOptions::default(),
        )
        .await
        .expect("metadata request must succeed on still-healthy connection");

    producer.close().await.unwrap();
}

/// Probe `(topic, partition 0)` on `leader` with `Fetch(max_wait_ms=0)`
/// until the broker reports `error_code == 0`. Both Produce and Fetch
/// flow through `ReplicaManager.getPartitionOrException`, so a clean
/// Fetch is a stand-in for "the leader's `ReplicaManager` has the
/// partition online" — the missing readiness signal that
/// `wait_for_topic_visible` does not cover.
async fn wait_for_partition_ready(client: &crate::Client, leader: BrokerId, topic: &TopicName) {
    // Generous safety net for a wedged broker. The expected path
    // converges in tens of ms; this only fires if the partition never
    // becomes online, which would be a Kafka-side bug worth surfacing
    // loudly rather than letting the test hang.
    const TIMEOUT: Duration = Duration::from_secs(30);
    let deadline = Instant::now() + TIMEOUT;
    let mut backoff = Duration::from_millis(20);

    loop {
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(-1))
            .with_max_wait_ms(0)
            .with_min_bytes(0)
            // 1 byte is enough: we only care about the per-partition
            // error code, not the records.
            .with_max_bytes(1)
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
                            .with_partition_max_bytes(1),
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
            .expect("readiness probe Fetch must succeed");

        let ready = resp.responses.iter().any(|tr| {
            tr.partitions
                .iter()
                .any(|pr| pr.partition_index == 0 && pr.error_code == 0)
        });
        if ready {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "partition 0 of topic '{}' never became ready for I/O within {TIMEOUT:?}",
            topic.as_str(),
        );

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_millis(200));
    }
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

async fn fetch_record_count(client: &crate::Client, leader: BrokerId, topic: &TopicName) -> usize {
    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(0)
        .with_max_bytes(1024 * 1024)
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
                        .with_partition_max_bytes(1024 * 1024),
                ]),
        ]);

    // v12: still uses topic-name (v13+ requires topic_id). Easier to
    // build by name than to plumb topic_id through the test.
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

    let mut count = 0;
    for tr in &resp.responses {
        for pr in &tr.partitions {
            let Some(records) = pr.records.as_ref() else {
                continue;
            };
            let mut cursor = Cursor::new(records.as_ref());
            // The broker may pack one or more record batches into a
            // single Fetch response; loop until the cursor is drained.
            while (cursor.position() as usize) < records.len() {
                match RecordBatchDecoder::decode(&mut cursor) {
                    Ok(batch) => count += batch.records.len(),
                    Err(e) => {
                        tracing::error!(error = ?e, "record batch decode failed");
                        break;
                    }
                }
            }
        }
    }
    count
}
