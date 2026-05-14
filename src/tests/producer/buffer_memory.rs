use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::client::PartitionId;
use crate::error::Error;
use crate::tests::helpers;
use crate::tests::helpers::create_topic;
use crate::tests::helpers::proxy::{Fault, FaultPlan, RequestView};

async fn connect() -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap()
}

/// `buffer_memory` is a hard cap on un-drained record bytes. Filling it
/// makes the next `Producer::send` park inside the accumulator's
/// `reserve` instead of buffering immediately; once the sender's linger
/// tick drains a batch and frees room, the parked send wakes, enqueues,
/// and acks normally. Proves backpressure is applied *and* released —
/// not a one-way wedge.
#[tokio::test]
async fn buffer_memory_blocks_then_unblocks() {
    let client = connect().await;
    let name = "producer-buffer-blocks-unblocks";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // 16 KiB buffer. batch_size_bytes is far larger, so the tail batch
    // never flags full and never rotates — the 500 ms linger tick is the
    // only thing that drains it, hence the only thing that frees buffer
    // room. max_block 30 s leaves the parked send ample time to wake.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_buffer_memory(16 * 1024)
            // max_record_size must not exceed buffer_memory (Producer::new
            // rejects that combination); 8 KiB still dwarfs the ~1 KiB
            // records below.
            .with_max_record_size(8 * 1024)
            .with_batch_size_bytes(1 << 20)
            .with_linger(Duration::from_millis(500))
            .with_max_block(Duration::from_secs(30)),
    )
    .expect("producer config");
    let producer = Arc::new(producer);

    // estimate_record_size = 24 B per-record overhead + value length, so
    // a 1 KiB value reserves ~1048 B. 15 records fit in 16 KiB
    // (15 × 1048 = 15720); the 16th cannot (16 × 1048 = 16768).
    const VALUE_LEN: usize = 1024;
    let mut accepted = Vec::new();
    for _ in 0..15 {
        let fut = producer
            .send(
                crate::ProducerRecord::new(topic.clone(), Bytes::from(vec![b'x'; VALUE_LEN]))
                    .with_partition(PartitionId(0)),
                None,
            )
            .await
            .expect("record fits while the buffer has room");
        accepted.push(fut);
    }

    // The 16th send cannot reserve buffer space — it must park.
    let blocked = tokio::spawn({
        let producer = producer.clone();
        let topic = topic.clone();
        async move {
            producer
                .send(
                    crate::ProducerRecord::new(topic, Bytes::from(vec![b'x'; VALUE_LEN]))
                        .with_partition(PartitionId(0)),
                    None,
                )
                .await
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !blocked.is_finished(),
        "send must park while buffer_memory is exhausted",
    );

    // The linger tick drains the buffered batch, freeing room and waking
    // the parked send.
    let unblocked = tokio::time::timeout(Duration::from_secs(10), blocked)
        .await
        .expect("parked send must wake once a drain frees buffer room")
        .expect("spawned send task panicked")
        .expect("send succeeds once buffer room is available");

    // Every record — the 15 that fit immediately and the 1 that waited —
    // acks end to end.
    for fut in accepted {
        tokio::time::timeout(Duration::from_secs(10), fut)
            .await
            .expect("buffered record must ack")
            .expect("ack");
    }
    tokio::time::timeout(Duration::from_secs(10), unblocked)
        .await
        .expect("unblocked record must ack")
        .expect("ack");

    Arc::try_unwrap(producer)
        .unwrap_or_else(|_| panic!("producer Arc still shared after the spawned task joined"))
        .close()
        .await
        .unwrap();
}

/// When `buffer_memory` is exhausted and nothing drains it, a
/// `Producer::send` parked in `reserve` must give up after `max_block`
/// and return `Error::BufferFull` rather than blocking forever. A
/// 1-hour linger guarantees the sender never drains during the test, so
/// the buffer stays full once filled.
#[tokio::test]
async fn buffer_memory_full_returns_buffer_full_after_max_block() {
    let client = connect().await;
    let name = "producer-buffer-full-times-out";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_buffer_memory(16 * 1024)
            // max_record_size must not exceed buffer_memory (Producer::new
            // rejects that combination); 8 KiB still dwarfs the ~1 KiB
            // records below.
            .with_max_record_size(8 * 1024)
            .with_batch_size_bytes(1 << 20)
            // 1-hour linger: the sender's only drain is the final
            // close(), so the buffer stays full for the whole test.
            .with_linger(Duration::from_secs(3600))
            .with_max_block(Duration::from_millis(200)),
    )
    .expect("producer config");

    const VALUE_LEN: usize = 1024;
    let payload = || Bytes::from(vec![b'x'; VALUE_LEN]);

    // Records pile into one non-full tail batch the linger never drains,
    // so `bytes_in_use` only climbs. The send that can no longer reserve
    // parks for `max_block` (200 ms) and then fails with BufferFull. The
    // 1000-iteration cap is just an infinite-loop guard — the buffer
    // fills after ~15 records.
    let mut accepted = Vec::new();
    let mut buffer_full = false;
    for _ in 0..1000 {
        match producer
            .send(
                crate::ProducerRecord::new(topic.clone(), payload()).with_partition(PartitionId(0)),
                None,
            )
            .await
        {
            Ok(fut) => accepted.push(fut),
            Err(Error::BufferFull(_)) => {
                buffer_full = true;
                break;
            }
            Err(other) => panic!("expected BufferFull once the buffer fills, got {other}"),
        }
    }
    assert!(
        buffer_full,
        "buffer_memory cap never triggered after 1000 sends",
    );
    assert!(
        !accepted.is_empty(),
        "some records must fit before the buffer fills",
    );

    // close() drains everything that did fit, so the buffered records
    // still ship on the way out.
    producer.close().await.unwrap();
}

/// T3b: `BufferFullPolicy::Error` sheds load synchronously. When
/// `buffer_memory` is exhausted, `Producer::send` fails immediately with
/// `Error::BufferFull` instead of parking up to `max_block`. The config
/// here gives a `Block`-policy producer a 30 s `max_block` and a 1-hour
/// linger (nothing ever drains), so a parking `send` would stall for the
/// full 30 s — under `Error` policy the whole fill-then-reject loop must
/// instead finish near-instantly.
#[tokio::test]
async fn buffer_full_policy_error_fails_fast_without_blocking() {
    let client = connect().await;
    let name = "producer-buffer-full-policy-error";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_buffer_memory(16 * 1024)
            .with_max_record_size(8 * 1024)
            .with_batch_size_bytes(1 << 20)
            // 1-hour linger: nothing drains the buffer for the whole test.
            .with_linger(Duration::from_secs(3600))
            // A long max_block: a Block-policy producer would park here
            // for the full 30 s. Error policy must not touch it.
            .with_max_block(Duration::from_secs(30))
            .with_buffer_full_policy(crate::BufferFullPolicy::Error),
    )
    .expect("producer config");

    const VALUE_LEN: usize = 1024;
    let payload = || Bytes::from(vec![b'x'; VALUE_LEN]);

    // Records pile into one non-full tail batch the linger never drains,
    // so `bytes_in_use` only climbs. Once a record can no longer reserve,
    // `Error` policy rejects it on the spot. The 1000-iteration cap is
    // just an infinite-loop guard — the buffer fills after ~15 records.
    let start = std::time::Instant::now();
    let mut accepted = Vec::new();
    let mut buffer_full = false;
    for _ in 0..1000 {
        match producer
            .send(
                crate::ProducerRecord::new(topic.clone(), payload()).with_partition(PartitionId(0)),
                None,
            )
            .await
        {
            Ok(fut) => accepted.push(fut),
            Err(Error::BufferFull(_)) => {
                buffer_full = true;
                break;
            }
            Err(other) => panic!("expected BufferFull once the buffer fills, got {other}"),
        }
    }
    assert!(
        buffer_full,
        "Error policy never surfaced BufferFull after 1000 sends",
    );
    assert!(
        !accepted.is_empty(),
        "some records must fit before the buffer fills",
    );
    // The decisive check: a Block-policy producer would have parked the
    // rejected send for the full 30 s max_block. Error policy fails fast.
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "Error policy must fail fast, not park up to max_block (took {elapsed:?})",
    );

    // close() drains everything that did fit, so the buffered records
    // still ship on the way out.
    producer.close().await.unwrap();
}

/// T2c: a batch that has drained out of the accumulator into the
/// idempotent in-flight queue still counts against `buffer_memory`. The
/// proxy tears down every Produce connection before it reaches the
/// broker, so dispatched batches never ack and stay pinned in the
/// in-flight queue; `delivery_timeout` is long enough that none are
/// dropped mid-test. After a first wave is `flush`ed — which returns
/// only once the accumulator is empty, i.e. every record has moved into
/// the in-flight queue — `bytes_in_use` is ~0 and the *only* thing
/// occupying `buffer_memory` is the in-flight queue. A fresh wave of
/// sends must therefore still hit the cap and fail with `BufferFull`.
/// Pre-T2c those in-flight bytes were invisible to `reserve`, so `send`
/// would never block — this test fails outright without the accounting.
#[tokio::test]
async fn idempotent_in_flight_counts_against_buffer_memory() {
    let upstream = helpers::plaintext_broker().await;

    // Drop every Produce connection before it reaches the broker: the
    // client never sees an ack, so each dispatched batch stays pinned
    // in the idempotent in-flight queue for the whole test.
    let plan = FaultPlan {
        on_request: Some(Arc::new(|view: &RequestView<'_>| {
            (view.api_key == ApiKey::Produce).then_some(Fault::DropConnection)
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

    let name = "producer-idempotent-inflight-buffer";
    create_topic(&client, name, 1).await;
    let topic = TopicName::from(StrBytes::from_static_str(name));

    // 8 KiB buffer. batch_size_bytes=1 rotates every record into its
    // own batch, so the linger tick drains each one straight into the
    // in-flight queue. delivery_timeout is long so nothing is dropped
    // from the in-flight queue mid-test; max_block is short so the
    // eventually-blocked send fails quickly.
    let producer = crate::Producer::new(
        client.clone(),
        crate::ProducerConfig::default()
            .with_enable_idempotence(true)
            .with_buffer_memory(8 * 1024)
            .with_max_record_size(4 * 1024)
            .with_batch_size_bytes(1)
            .with_linger(Duration::from_millis(1))
            .with_max_block(Duration::from_millis(300))
            .with_delivery_timeout(Duration::from_secs(120)),
    )
    .expect("producer config");

    const VALUE_LEN: usize = 256;
    let payload = || Bytes::from(vec![b'x'; VALUE_LEN]);

    // First wave: 8 records comfortably fit the empty 8 KiB buffer.
    let mut first_wave = Vec::new();
    for _ in 0..8 {
        let fut = producer
            .send(
                crate::ProducerRecord::new(topic.clone(), payload()).with_partition(PartitionId(0)),
                None,
            )
            .await
            .expect("first-wave record fits the empty buffer");
        first_wave.push(fut);
    }

    // `flush` returns once the accumulator is empty — i.e. once every
    // first-wave record has drained out of the accumulator and into the
    // idempotent in-flight queue. After this point `bytes_in_use` is
    // ~0; the in-flight queue holds those 8 batches' worth of bytes.
    tokio::time::timeout(Duration::from_secs(10), producer.flush())
        .await
        .expect("accumulator must drain into the in-flight queue")
        .expect("flush");

    // Second wave: pre-T2c the in-flight bytes were invisible to
    // `reserve`, so every one of these would succeed. With T2c they
    // count against the cap, so once `inflight_bytes + bytes_in_use`
    // saturates 8 KiB a send must park for `max_block` and fail with
    // `BufferFull`. The 1000-iteration cap is just an infinite-loop
    // guard — nothing ever releases budget (no ack, no delivery
    // timeout), so the total climbs monotonically and the cap is hit
    // within a few dozen records.
    let mut accepted = 0usize;
    let mut buffer_full = false;
    for _ in 0..1000 {
        match producer
            .send(
                crate::ProducerRecord::new(topic.clone(), payload()).with_partition(PartitionId(0)),
                None,
            )
            .await
        {
            Ok(_fut) => accepted += 1,
            Err(Error::BufferFull(_)) => {
                buffer_full = true;
                break;
            }
            Err(other) => panic!("expected BufferFull once the buffer fills, got {other}"),
        }
    }
    assert!(
        accepted > 0,
        "the buffer had room right after flush — at least one second-wave send must fit",
    );
    assert!(
        buffer_full,
        "in-flight queue bytes never triggered buffer_memory backpressure — \
         T2c accounting is missing or broken",
    );

    drop(first_wave);
    producer.close().await.unwrap();
}
