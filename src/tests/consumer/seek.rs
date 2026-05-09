use std::collections::HashSet;
use std::time::{Duration, Instant};

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
async fn seek_replays_from_offset_zero() {
    let client = connect().await;
    let name = "consumer-seek";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // linger = 0 (floored to the sender's 1ms tick) keeps the produce
    // phase below the collect deadline without relying on size-based
    // batch wakeups. Same shape as the round_trip / earliest_latest tests.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default().with_linger(Duration::from_millis(0)),
    )
    .expect("producer config");

    const N: usize = 10;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        let payload = Bytes::from(format!("seek-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }
    // Block until every record is durably written so the consumer's
    // start-offset resolution sees a stable HWM at offset N.
    for fut in futures {
        fut.await.expect("ack");
    }

    let mut consumer = crate::Consumer::new(client.clone(), crate::ConsumerConfig::default());
    let tp = topic_partition(name, 0);
    consumer.assign(vec![tp.clone()]).await.expect("assign");

    // Drain the first 5 records — natural sequence from the start of the log.
    const TAKE: usize = 5;
    let initial = collect_n(&mut consumer, TAKE, Duration::from_secs(30)).await;
    for (i, rec) in initial.iter().enumerate() {
        assert_eq!(rec.topic, topic);
        assert_eq!(rec.partition, PartitionId(0));
        assert_eq!(
            rec.offset, i as i64,
            "initial drain offsets must be dense 0..TAKE"
        );
    }

    // Reposition the cursor back to offset 0. `seek` is fire-and-forget;
    // the fetcher applies the update on its next loop iteration (or on
    // its mid-emit `select!` if it's currently parked on `record_tx.reserve`).
    consumer.seek(tp.clone(), 0).expect("seek");

    // The internal record channel may still hold up to N - TAKE = 5
    // records buffered before the seek arrived (offsets TAKE..N).
    // Those leftovers are emitted before the post-seek refetch lands, so
    // the worst-case stream after seek is `[TAKE..N) ++ [0..N)` =
    // 5 + 10 = 15 records. Stop early once we've both observed offset 0
    // (proving the seek replayed) and seen every offset 0..N.
    let mut seen: HashSet<i64> = HashSet::new();
    let mut zero_seen_after_seek = false;
    let mut total_after_seek = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !zero_seen_after_seek || seen.len() < N {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_else(|| {
                panic!(
                    "seek drain timed out: zero_seen={zero_seen_after_seek} \
                 seen={seen:?} count={total_after_seek}"
                )
            });
        let rec = match tokio::time::timeout(remaining, consumer.recv()).await {
            Ok(Some(Ok(r))) => r,
            Ok(Some(Err(e))) => panic!("consumer yielded error after seek: {e:?}"),
            Ok(None) => panic!("consumer stream ended after seek"),
            Err(_) => panic!(
                "seek drain timed out: zero_seen={zero_seen_after_seek} \
                 seen={seen:?} count={total_after_seek}"
            ),
        };
        assert_eq!(rec.topic, topic);
        assert_eq!(rec.partition, PartitionId(0));
        // Every offset must be in the produced range — no phantom records.
        assert!(
            (0..N as i64).contains(&rec.offset),
            "unexpected offset {} after seek",
            rec.offset
        );
        if rec.offset == 0 {
            zero_seen_after_seek = true;
        }
        seen.insert(rec.offset);
        total_after_seek += 1;
        // Hard cap: at most leftovers (N - TAKE) + replay (N) = 15.
        // A higher count means the fetcher is replaying more than once,
        // which would indicate seek did not "stick".
        assert!(
            total_after_seek <= (N - TAKE) + N,
            "too many records after seek ({total_after_seek}); seek did not stop replaying"
        );
    }

    producer.close().await.unwrap();
    consumer.close().await.unwrap();
}
