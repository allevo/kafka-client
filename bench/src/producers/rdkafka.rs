//! `BenchProducer` for `rdkafka` (the librdkafka C wrapper) — the closest
//! analog to this library's producer: a background queue with `linger.ms`
//! / `batch.size`.
//!
//! Inherent asymmetry, noted in the methodology footer rather than
//! "corrected" for: librdkafka copies each payload into its own C-side
//! queue, so the `Bytes` we hand it is duplicated before the send returns.

use std::time::{Duration, Instant};

use bytes::Bytes;
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};
use rdkafka::util::Timeout;

use crate::bench_producer::BenchProducer;
use crate::workload::{AcksLevel, Workload};

pub struct RdkafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl RdkafkaProducer {
    pub fn connect(workload: &Workload) -> anyhow::Result<Self> {
        let acks = match workload.acks {
            AcksLevel::None => "0",
            AcksLevel::Leader => "1",
            AcksLevel::All => "all",
        };
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &workload.bootstrap)
            .set("acks", acks)
            .set("linger.ms", workload.linger.as_millis().to_string())
            .set("batch.size", workload.batch_size_bytes.to_string())
            .create()?;
        Ok(Self {
            producer,
            topic: workload.topic.clone(),
        })
    }
}

impl BenchProducer for RdkafkaProducer {
    async fn send_one(&self, payload: Bytes) -> anyhow::Result<Duration> {
        // `Timeout::Never`: block on a full queue rather than fail, so the
        // outstanding-ack depth is governed solely by the shared runner.
        // The future resolves on the broker ack.
        let started = Instant::now();
        let record = FutureRecord::<(), [u8]>::to(&self.topic).payload(&payload);
        self.producer
            .send(record, Timeout::Never)
            .await
            .map_err(|(e, _)| anyhow::anyhow!("rdkafka send failed: {e}"))?;
        Ok(started.elapsed())
    }

    async fn finalize(self) -> anyhow::Result<()> {
        self.producer.flush(Timeout::Never)?;
        Ok(())
    }
}
