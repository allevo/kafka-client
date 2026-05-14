//! `BenchProducer` for `rskafka` (pure Rust).
//!
//! rskafka's `PartitionClient::produce` is lower-level — no background
//! linger. We use its `BatchProducer` (linger + size-bounded aggregator)
//! as the primary impl so all four crates have a batching story.
//!
//! Asymmetries, surfaced in the methodology footer:
//! - `BatchProducer` / `PartitionClient` are per-partition; this impl
//!   produces only to **partition 0**. With `--partitions > 1` it does not
//!   spread load the way `this` / rdkafka do.
//! - `PartitionClient::produce` exposes no per-produce `acks` knob — it is
//!   effectively `acks=all`. For `--acks none|leader` runs the rskafka
//!   column does not honor the requested level.
//! - `rskafka::record::Record` holds `Vec<u8>`, so the payload `Bytes` is
//!   copied on every send.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rskafka::client::ClientBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::producer::aggregator::RecordAggregator;
use rskafka::client::producer::{BatchProducer, BatchProducerBuilder};
use rskafka::record::Record;

use crate::bench_producer::BenchProducer;
use crate::workload::Workload;

/// Partition this impl produces to — see the module-level asymmetry note.
const PARTITION: i32 = 0;

pub struct RskafkaProducer {
    batch_producer: BatchProducer<RecordAggregator>,
}

impl RskafkaProducer {
    pub async fn connect(workload: &Workload) -> anyhow::Result<Self> {
        let client = ClientBuilder::new(vec![workload.bootstrap.clone()])
            .build()
            .await?;
        // `UnknownTopicHandling::Retry`: the topic was just created by the
        // orchestrator; tolerate the brief metadata-propagation window.
        let partition_client = Arc::new(
            client
                .partition_client(
                    workload.topic.clone(),
                    PARTITION,
                    UnknownTopicHandling::Retry,
                )
                .await?,
        );
        let batch_producer = BatchProducerBuilder::new(partition_client)
            .with_linger(workload.linger)
            .with_compression(Compression::NoCompression)
            .build(RecordAggregator::new(workload.batch_size_bytes));
        Ok(Self { batch_producer })
    }
}

impl BenchProducer for RskafkaProducer {
    async fn send_one(&self, payload: Bytes) -> anyhow::Result<Duration> {
        // `chrono::Utc::now()` needs chrono's `clock` feature, which
        // rskafka's re-export doesn't enable — build the timestamp from
        // the system clock instead.
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let record = Record {
            key: None,
            value: Some(payload.to_vec()),
            headers: BTreeMap::new(),
            timestamp: rskafka::chrono::DateTime::from_timestamp_millis(now_ms).unwrap_or_default(),
        };
        // `produce` resolves once the batch carrying this record is acked.
        let started = Instant::now();
        self.batch_producer.produce(record).await?;
        Ok(started.elapsed())
    }

    async fn finalize(self) -> anyhow::Result<()> {
        self.batch_producer.flush().await?;
        Ok(())
    }
}
