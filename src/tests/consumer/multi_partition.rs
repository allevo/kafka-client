use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{BrokerId, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::client::PartitionId;
use crate::tests::helpers;
use crate::tests::helpers::{collect_n, create_topic, topic_partition};

/// Bootstrap against the shared 3-node KRaft cluster. Same shape as
/// `cluster::connected_3node_client`: a custom resolver maps every
/// broker id back to its container-external host:port (the metadata-
/// advertised internal hostnames aren't reachable from the test host),
/// then we wait for metadata to surface all 3 brokers before returning.
async fn connect_cluster() -> crate::Client {
    let cluster = helpers::plaintext_cluster().await;
    let addr_map = cluster.addr_map.clone();
    let (ref boot_host, boot_port) = addr_map[&BrokerId(1)];
    let bootstrap = [crate::Config::new(boot_host, boot_port)];

    let resolver_map = addr_map.clone();
    let client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |node_id, _host, _port| Ok(resolver_map[&node_id].clone()),
    )
    .await
    .unwrap();

    let mut metadata = client.refresh_metadata().await.unwrap();
    for _ in 0..10 {
        if metadata.brokers.len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        metadata = client.refresh_metadata().await.unwrap();
    }
    assert_eq!(metadata.brokers.len(), 3);
    client
}

#[tokio::test]
#[tracing_test::traced_test]
async fn round_trip_three_partitions() {
    let client = connect_cluster().await;
    let name = "consumer-multi-partition";
    create_topic(&client, name, 3).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    );

    // Pin records to specific partitions via `with_partition` rather than
    // relying on key-hashing to land them on all 3 partitions: the murmur2
    // partitioner would still produce a deterministic mapping, but coupling
    // this test to a specific (key, partition_count) hash output makes the
    // test brittle to partitioner changes. The fetcher's per-leader grouping
    // is what's under test, not the partitioner.
    const N_PER_PART: usize = 4;
    const PARTS: i32 = 3;
    let mut futures = Vec::new();
    let mut expected: HashMap<PartitionId, Vec<Bytes>> = HashMap::new();
    for i in 0..(N_PER_PART as i32 * PARTS) {
        let partition = PartitionId(i % PARTS);
        let payload = Bytes::from(format!("multi-p{}-i{i}", partition.0));
        expected.entry(partition).or_default().push(payload.clone());
        let fut = producer
            .send(
                crate::ProducerRecord::new(topic.clone(), payload).with_partition(partition),
                None,
            )
            .await
            .expect("enqueue");
        futures.push(fut);
    }
    // Block on every ack: start-offset resolution at assign time reads each
    // partition's HWM, and we want all records visible before that snapshot.
    for fut in futures {
        fut.await.expect("ack");
    }

    let consumer = crate::Consumer::new(client.clone(), crate::ConsumerConfig::default());
    let tps: Vec<_> = (0..PARTS).map(|p| topic_partition(name, p)).collect();
    consumer.assign(tps).await.expect("assign");

    let total = N_PER_PART * PARTS as usize;
    let records = collect_n(&consumer, total, Duration::from_secs(60)).await;

    // Order across partitions is unspecified (the fetcher polls leaders in
    // parallel), so bucket per-partition before asserting. Within a single
    // partition the producer appended sequentially and `acks=All` blocked
    // each send, so we can compare to the per-partition produce order
    // verbatim.
    let mut by_partition: HashMap<PartitionId, Vec<Bytes>> = HashMap::new();
    for rec in &records {
        assert_eq!(rec.topic, topic);
        let payload = rec.value.as_ref().expect("value present").clone();
        by_partition.entry(rec.partition).or_default().push(payload);
    }

    for p in 0..PARTS {
        let pid = PartitionId(p);
        let got = by_partition
            .remove(&pid)
            .unwrap_or_else(|| panic!("no records observed for partition {p}"));
        let want = expected
            .remove(&pid)
            .unwrap_or_else(|| panic!("expected map missing partition {p}"));
        assert_eq!(got, want, "partition {p} record mismatch");
    }
    assert!(
        by_partition.is_empty(),
        "unexpected records on partitions outside 0..{PARTS}: {by_partition:?}"
    );

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}
