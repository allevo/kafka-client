use std::collections::HashSet;
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{ApiKey, BrokerId, FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::RecordBatchDecoder;

use crate::client::PartitionId;
use crate::tests::helpers;
use crate::tests::helpers::create_topic;
use crate::tests::helpers::proxy::{
    Fault, FaultPlan, RequestHook, RequestView, ResponseHook, ResponseView,
};

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

/// `enable_idempotence = true` must coerce `acks=All`, cap
/// `max_in_flight_per_broker` at 5, and raise `retries` from 0 to
/// `u32::MAX` — matching Java's `KafkaProducer` validation. Verified
/// against the producer's effective config without going near a broker.
#[tokio::test]
async fn auto_coercion_applies_idempotence_defaults() {
    let client = connect().await;
    let producer = crate::Producer::new(
        client,
        crate::ProducerConfig::default()
            .with_enable_idempotence(true)
            .with_acks(crate::Acks::Leader)
            .with_max_in_flight_per_broker(16)
            .with_retries(0),
    )
    .expect("idempotent producer must build with valid acks");

    let cfg = producer.effective_config();
    assert!(matches!(cfg.acks, crate::Acks::All));
    assert_eq!(cfg.max_in_flight_per_broker, 5);
    assert_eq!(cfg.retries, u32::MAX);
    assert!(cfg.enable_idempotence);

    producer.close().await.unwrap();
}

/// `acks=None` + `enable_idempotence=true` is a configuration error —
/// matches Java's `KafkaProducer.configureAcks` which throws
/// `ConfigException` for the same combination.
#[tokio::test]
async fn idempotence_with_acks_none_returns_config_error() {
    let client = connect().await;
    let err = crate::Producer::new(
        client,
        crate::ProducerConfig::default()
            .with_enable_idempotence(true)
            .with_acks(crate::Acks::None),
    )
    .err()
    .expect("acks=None must be rejected when idempotence is on");
    assert!(matches!(err, crate::Error::Config(_)), "unexpected: {err}");
}

/// Happy-path producer-id acquisition + per-batch sequence stamping.
/// Sends three batches on a single partition, then re-fetches the raw
/// record-batch v2 frames and asserts that `producer_id != -1`,
/// `producer_epoch >= 0`, and `base_sequence` runs 0, N, 2N — proving
/// the wire format carries the broker-assigned producer-id and
/// monotonic per-partition sequence space.
#[tokio::test]
#[tracing_test::traced_test]
async fn idempotent_producer_id_and_sequence_round_trip() {
    let client = connect().await;
    let name = "producer-idempotent-sequence";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // batch_size_bytes=1 forces every append to flush immediately, so
    // each record ships in its own RecordBatch frame and we get
    // base_sequence = 0, 1, 2. linger=0 keeps it snappy.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_enable_idempotence(true)
            .with_linger(Duration::from_millis(0))
            .with_batch_size_bytes(1),
    )
    .expect("producer config");

    const N: usize = 6;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        let payload = Bytes::from(format!("idempotent-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }
    for fut in futures {
        fut.await.expect("ack");
    }

    let leader = leader_for(&client, &topic).await;
    let batches = fetch_decoded_batches(&client, leader, &topic).await;

    assert!(
        !batches.is_empty(),
        "expected at least one batch on partition 0"
    );

    // Each batch gets its own record (batch_size_bytes=1 forces full
    // on every append). Sequence numbers run 0..N across batches.
    let mut seen_seq = 0;
    let mut last_pid: Option<i64> = None;
    let mut last_epoch: Option<i16> = None;
    for batch in batches {
        for r in batch.records {
            assert!(
                r.producer_id != -1,
                "record producer_id stays unset after idempotent send"
            );
            assert!(
                r.producer_epoch >= 0,
                "record producer_epoch must be non-negative after init",
            );
            assert_eq!(r.sequence, seen_seq, "per-record sequence must be dense");
            seen_seq += 1;

            // All records share the same (producer_id, producer_epoch)
            // — this is one producer instance, no fencing.
            match (last_pid, last_epoch) {
                (Some(p), Some(e)) => {
                    assert_eq!(r.producer_id, p);
                    assert_eq!(r.producer_epoch, e);
                }
                _ => {
                    last_pid = Some(r.producer_id);
                    last_epoch = Some(r.producer_epoch);
                }
            }
        }
    }
    assert_eq!(seen_seq as usize, N, "expected {N} records, saw {seen_seq}");

    producer.close().await.unwrap();
}

/// Drop the first Produce request through the proxy, then forward
/// every subsequent one. With idempotence on, the producer must
/// retransmit the in-flight bytes (same `producer_id` + `base_sequence`)
/// after the broker tears the dialled-through-proxy connection down,
/// the broker dedups, and the on-disk record count comes out exactly
/// equal to N — no duplicates, no losses.
#[tokio::test]
#[tracing_test::traced_test]
async fn idempotent_retry_after_dropped_produce_survives_with_no_duplicates() {
    let upstream = helpers::plaintext_broker().await;

    // Drop *exactly* the first ProduceRequest to force the producer's
    // retry path. Subsequent Produce attempts get forwarded so the
    // test can converge.
    let dropped = Arc::new(AtomicUsize::new(0));
    let hook = dropped.clone();
    let plan = FaultPlan {
        on_request: Some(Arc::new(move |view: &RequestView<'_>| {
            if view.api_key == ApiKey::Produce
                && hook
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                Some(Fault::DropConnection)
            } else {
                None
            }
        })),
        on_response: None,
    };
    let proxy = helpers::proxy::start(&upstream.host, upstream.port, plan).await;

    let bootstrap = [crate::Config::new(&proxy.host, proxy.port)];
    let proxy_host = proxy.host.clone();
    let proxy_port = proxy.port;
    let client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |_id, _host, _port| Ok((proxy_host.clone(), proxy_port)),
    )
    .await
    .unwrap();

    let name = "producer-idempotent-retry";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_enable_idempotence(true)
            .with_linger(Duration::from_millis(0))
            .with_delivery_timeout(Duration::from_secs(60)),
    )
    .expect("producer config");

    const N: usize = 8;
    let mut futures = Vec::with_capacity(N);
    for i in 0..N {
        let payload = Bytes::from(format!("retry-{i}"));
        let fut = producer
            .send(crate::ProducerRecord::new(topic.clone(), payload), None)
            .await
            .expect("enqueue");
        futures.push(fut);
    }

    // Generous per-future budget so a single in-flight retry's
    // 30s broker-side timeout doesn't time the test out before the
    // sender's next tick redispatches the same encoded bytes.
    for fut in futures {
        tokio::time::timeout(Duration::from_secs(60), fut)
            .await
            .expect("future did not resolve within 60s — retry path stalled")
            .expect("ack");
    }

    // Confirm the proxy actually dropped a Produce request.
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "proxy fault hook never fired — drop path not exercised",
    );

    let leader = leader_for(&client, &topic).await;
    let batches = fetch_decoded_batches(&client, leader, &topic).await;
    let total: usize = batches.iter().map(|b| b.records.len()).sum();
    assert_eq!(
        total, N,
        "broker log has {total} records, expected exactly {N} — duplicate or loss",
    );

    producer.close().await.unwrap();
}

/// Drop *every* Produce request through the proxy until `delivery_timeout`
/// fires, restore the path, then attempt a follow-up send. The pre-B1
/// behaviour was to silently advance `next_sequence` on the local pop,
/// then trip `OutOfOrderSequence` on the next send. Post-Phase-C, the
/// timeout fails the head batch *and* the bump pass recovers the
/// partition under a fresh `(producer_id, producer_epoch)` — so the
/// follow-up send must succeed against the restored upstream path,
/// proving the partition is no longer poisoned.
#[tokio::test]
#[tracing_test::traced_test]
async fn delivery_timeout_does_not_corrupt_partition() {
    let upstream = helpers::plaintext_broker().await;

    let drop_produce = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let hook = drop_produce.clone();
    let plan = FaultPlan {
        on_request: Some(Arc::new(move |view: &RequestView<'_>| {
            if view.api_key == ApiKey::Produce && hook.load(Ordering::SeqCst) {
                Some(Fault::DropConnection)
            } else {
                None
            }
        })),
        on_response: None,
    };
    let proxy = helpers::proxy::start(&upstream.host, upstream.port, plan).await;

    let bootstrap = [crate::Config::new(&proxy.host, proxy.port)];
    let proxy_host = proxy.host.clone();
    let proxy_port = proxy.port;
    let client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |_id, _host, _port| Ok((proxy_host.clone(), proxy_port)),
    )
    .await
    .unwrap();

    let name = "producer-idempotent-delivery-timeout";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // delivery_timeout is short so the test runs in a few seconds; the
    // sender keeps re-dispatching the in-flight bytes at linger cadence
    // until the wall-clock deadline trips.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_enable_idempotence(true)
            .with_linger(Duration::from_millis(0))
            .with_delivery_timeout(Duration::from_secs(3)),
    )
    .expect("producer config");

    let fut1 = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"first")),
            None,
        )
        .await
        .expect("enqueue first");

    // delivery_timeout (3s) + slack for the bump short-circuit to land
    // on the next dispatch tick. A hang here means the local pop left
    // a sequence gap that we never recovered — the original C1 bug.
    let outcome1 = tokio::time::timeout(Duration::from_secs(15), fut1)
        .await
        .expect("first record's future never resolved — sequence-gap poison?");
    let err1 = outcome1.expect_err("expected delivery_timeout to fail the first send");
    assert!(
        matches!(
            err1,
            crate::Error::RequestTimeout(_) | crate::Error::IdempotenceFenced(_)
        ),
        "unexpected error variant: {err1:?}",
    );

    // Restore the upstream path. The bump pass (Phase C) recovers the
    // partition under a fresh PID/epoch, so the follow-up send must
    // succeed — pinning the post-C1 invariant that a transient
    // delivery_timeout fails one batch but doesn't kill the producer.
    drop_produce.store(false, Ordering::SeqCst);

    let fut2 = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"second")),
            None,
        )
        .await
        .expect("enqueue second");
    let outcome2 = tokio::time::timeout(Duration::from_secs(10), fut2)
        .await
        .expect("second record stalled — partition not recovered by bump pass");
    let meta2 = outcome2.expect("second send must succeed after bump-pass recovery");
    assert_eq!(meta2.topic, topic);
    assert_eq!(meta2.partition, PartitionId(0));
    // Producer is not in fatal state — successive sends still work.
    let fut3 = producer
        .send(
            crate::ProducerRecord::new(topic.clone(), Bytes::from_static(b"third")),
            None,
        )
        .await
        .expect("enqueue third");
    let outcome3 = tokio::time::timeout(Duration::from_secs(10), fut3)
        .await
        .expect("third record stalled");
    outcome3.expect("third send must also succeed");

    producer.close().await.unwrap();
}

/// End-to-end exactly-once check with fault injection: produce N=1000
/// records to a single-partition topic through the proxy, drop one
/// Produce *request* and one Produce *response* mid-stream, and assert
/// the broker log converges to exactly N records with monotonic
/// offsets and unique `(producer_id, sequence)` tuples — *and* that the
/// producer's logs prove it actually traversed both idempotency
/// recovery paths.
///
/// Two deliberately distinct drop sites:
/// - **Request-side drop** (3rd Produce request): `Fault::DropConnection`
///   tears the socket *before* the frame reaches the broker, so the
///   broker never sees it. The client's `Client::send` fails with a
///   transport error and the batch stays in the in-flight queue.
/// - **Response-side drop** (6th Produce response): the request *did*
///   reach the broker and the records *were* committed; the proxy eats
///   the response before the client sees it. From the client's side
///   this is still a transport error, so the batch is retransmitted —
///   and the broker, finding the duplicate batch in its recent-batch
///   cache, returns the *original* offset as a plain success (it does
///   not surface `DuplicateSequenceNumber`; that code is reserved for
///   stale duplicates outside the broker's 5-batch window, per
///   `UnifiedLog.java:1399-1407` / `Sender.java:700-705`).
///
/// Either way the client re-dispatches the same `(producer_id,
/// base_sequence)` bytes — the core idempotent-retry action — logged
/// once per re-dispatch as `"retransmitting idempotent batch"`. The
/// `tracing-test` `logs_contain` assertions at the end pin both the
/// transport-failure observation (`"produce request to leader failed"`)
/// and the retransmit action: a regression that silently turned the
/// drops into no-ops, or that re-sent under a *fresh* sequence instead
/// of retransmitting, fails here even though the on-disk record count
/// might still look correct.
///
/// The test sends in chunks of 50 with an await between chunks so the
/// sender drains the current `PartitionBatch` before the next chunk's
/// appends start a fresh one — the in-process accumulator doesn't
/// rotate batches on `batch_size_bytes`, so without these awaits 1000
/// records would coalesce into a single Produce frame and neither drop
/// would land mid-stream.
#[tokio::test]
#[tracing_test::traced_test]
async fn idempotent_exactly_once_under_proxy_failure() {
    let upstream = helpers::plaintext_broker().await;

    // `produce_correlations` is the set of correlation ids currently
    // belonging to a Produce request. The response hook only sees
    // correlation ids — not api keys — so it consults this set to tell
    // a Produce response apart from a Metadata/ApiVersions one.
    // Correlation ids reset per connection (and the test reconnects on
    // every drop), so a non-Produce request reusing an id reclassifies
    // it via the `remove` below, keeping the set accurate.
    let produce_correlations: Arc<Mutex<HashSet<i32>>> = Arc::new(Mutex::new(HashSet::new()));
    let produce_request_count = Arc::new(AtomicUsize::new(0));
    let produce_response_count = Arc::new(AtomicUsize::new(0));
    let req_dropped = Arc::new(AtomicUsize::new(0));
    let resp_dropped = Arc::new(AtomicUsize::new(0));

    let corr_req = produce_correlations.clone();
    let req_count = produce_request_count.clone();
    let req_drop = req_dropped.clone();
    let on_request: RequestHook = Arc::new(move |view: &RequestView<'_>| -> Option<Fault> {
        if view.api_key == ApiKey::Produce {
            corr_req.lock().unwrap().insert(view.correlation_id);
            // Drop the 3rd Produce request *before* it reaches the
            // broker: the client never sees a response and must
            // retransmit on a fresh connection (TRANSPORT_DROP path).
            if req_count.fetch_add(1, Ordering::SeqCst) == 2 {
                req_drop.fetch_add(1, Ordering::SeqCst);
                return Some(Fault::DropConnection);
            }
        } else {
            corr_req.lock().unwrap().remove(&view.correlation_id);
        }
        None
    });

    let corr_resp = produce_correlations.clone();
    let resp_count = produce_response_count.clone();
    let resp_drop = resp_dropped.clone();
    let on_response: ResponseHook = Arc::new(move |view: &ResponseView<'_>| -> Option<Fault> {
        if corr_resp.lock().unwrap().contains(&view.correlation_id) {
            // Drop the 6th Produce *response*: the broker has by now
            // committed those records, so the client's retransmit is
            // deduped broker-side — DuplicateSequenceNumber (DEDUP path).
            if resp_count.fetch_add(1, Ordering::SeqCst) == 5 {
                resp_drop.fetch_add(1, Ordering::SeqCst);
                return Some(Fault::DropConnection);
            }
        }
        None
    });

    let plan = FaultPlan {
        on_request: Some(on_request),
        on_response: Some(on_response),
    };
    let proxy = helpers::proxy::start(&upstream.host, upstream.port, plan).await;

    let bootstrap = [crate::Config::new(&proxy.host, proxy.port)];
    let proxy_host = proxy.host.clone();
    let proxy_port = proxy.port;
    let client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |_id, _host, _port| Ok((proxy_host.clone(), proxy_port)),
    )
    .await
    .unwrap();

    let name = "producer-idempotent-fault-injection";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_enable_idempotence(true)
            .with_linger(Duration::from_millis(0))
            .with_delivery_timeout(Duration::from_secs(120)),
    )
    .expect("producer config");

    const N: usize = 1000;
    const CHUNK: usize = 50;
    for start in (0..N).step_by(CHUNK) {
        let end = (start + CHUNK).min(N);
        let mut chunk_futures = Vec::with_capacity(end - start);
        for i in start..end {
            let payload = Bytes::from(format!("fault-{i:04}"));
            let fut = producer
                .send(crate::ProducerRecord::new(topic.clone(), payload), None)
                .await
                .expect("enqueue");
            chunk_futures.push(fut);
        }
        // Await within the chunk so the sender drains the current
        // partition batch before the next chunk starts a new one.
        // Generous per-future budget because a chunk may carry both
        // its own dispatch *and* the retransmission of an earlier
        // dropped chunk when drops cluster temporally.
        for (idx, fut) in chunk_futures.into_iter().enumerate() {
            tokio::time::timeout(Duration::from_secs(120), fut)
                .await
                .unwrap_or_else(|_| panic!("record {} did not resolve within 120s", start + idx))
                .unwrap_or_else(|e| panic!("record {} failed: {e:?}", start + idx));
        }
    }

    // Both drops must have fired exactly once; otherwise the test
    // silently degraded to a happy-path round-trip and never exercised
    // retry recovery.
    assert_eq!(
        req_dropped.load(Ordering::SeqCst),
        1,
        "expected exactly 1 request-side Produce drop; saw {} Produce requests total",
        produce_request_count.load(Ordering::SeqCst),
    );
    assert_eq!(
        resp_dropped.load(Ordering::SeqCst),
        1,
        "expected exactly 1 response-side Produce drop; saw {} Produce responses total",
        produce_response_count.load(Ordering::SeqCst),
    );

    let leader = leader_for(&client, &topic).await;
    let batches = fetch_decoded_batches(&client, leader, &topic).await;

    // Exactly N records, monotonic offsets from 0, unique
    // (producer_id, sequence) tuples — broker-side dedup proof.
    let mut expected_offset = 0i64;
    let mut count = 0usize;
    let mut seen: HashSet<(i64, i32)> = HashSet::with_capacity(N);
    for batch in &batches {
        for r in &batch.records {
            assert_eq!(
                r.offset, expected_offset,
                "non-monotonic offset at index {count}",
            );
            expected_offset += 1;
            assert!(
                seen.insert((r.producer_id, r.sequence)),
                "duplicate (producer_id={}, sequence={}) at index {count}",
                r.producer_id,
                r.sequence,
            );
            count += 1;
        }
    }
    assert_eq!(count, N, "expected exactly {N} records on disk, found {count}");

    // The on-disk count proves the *outcome* is exactly-once; these two
    // assertions prove the producer actually *traversed* the recovery
    // path to get there. Each argument is the entire static message
    // body of its `tracing::` call site, so the assertion can only pass
    // if exactly that log site fired.
    assert!(
        logs_contain("produce request to leader failed"),
        "transport-failure log not seen — neither drop surfaced as a transport error to the client",
    );
    assert!(
        logs_contain("retransmitting idempotent batch"),
        "retransmit log not seen — the producer never re-dispatched an in-flight batch, so the idempotent-retry path was not exercised",
    );

    producer.close().await.unwrap();
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

async fn fetch_decoded_batches(
    client: &crate::Client,
    leader: BrokerId,
    topic: &TopicName,
) -> Vec<kafka_protocol::records::RecordSet> {
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

    let mut out = Vec::new();
    for tr in &resp.responses {
        for pr in &tr.partitions {
            let Some(records) = pr.records.as_ref() else {
                continue;
            };
            let mut cursor = Cursor::new(records.as_ref());
            while (cursor.position() as usize) < records.len() {
                match RecordBatchDecoder::decode(&mut cursor) {
                    Ok(batch) => out.push(batch),
                    Err(e) => {
                        tracing::error!(error = ?e, "record batch decode failed");
                        break;
                    }
                }
            }
        }
    }
    out
}
