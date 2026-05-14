//! `BenchProducer` for this library (`kafka-client`).
//!
//! Background accumulator + linger/batching — directly comparable to
//! rdkafka. Because it uses a `zeropool` buffer pool, the harness's warmup
//! phase matters specifically for this crate (first batches pay the pool's
//! cold-allocation cost).

use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_client::{
    Acks, Auth, Client, Config, Producer, ProducerConfig, ProducerRecord, Security, StrBytes,
    TopicName,
};

use crate::bench_producer::BenchProducer;
use crate::producers::parse_host_port;
use crate::workload::{AcksLevel, Workload};

pub struct ThisProducer {
    producer: Producer,
    topic: TopicName,
}

impl ThisProducer {
    pub async fn connect(workload: &Workload) -> anyhow::Result<Self> {
        let (host, port) = parse_host_port(&workload.bootstrap)?;
        let bootstrap = [Config::new(host, port)];
        let client = Client::connect(&bootstrap, Security::Plaintext, Auth::None).await?;

        let acks = match workload.acks {
            AcksLevel::None => Acks::None,
            AcksLevel::Leader => Acks::Leader,
            AcksLevel::All => Acks::All,
        };
        let config = ProducerConfig::default()
            .with_acks(acks)
            .with_linger(workload.linger)
            .with_batch_size_bytes(workload.batch_size_bytes);
        let producer = Producer::new(client, config)?;

        let topic = TopicName::from(StrBytes::from_string(workload.topic.clone()));
        Ok(Self { producer, topic })
    }
}

impl BenchProducer for ThisProducer {
    async fn send_one(&self, payload: Bytes) -> anyhow::Result<Duration> {
        // `send` resolves once the record is enqueued; the `SendFuture` it
        // returns is what actually carries the broker ack.
        let started = Instant::now();
        let ack = self
            .producer
            .send(ProducerRecord::new(self.topic.clone(), payload), None)
            .await?;
        ack.await?;
        Ok(started.elapsed())
    }

    async fn finalize(self) -> anyhow::Result<()> {
        self.producer.flush().await?;
        self.producer.close().await?;
        Ok(())
    }
}
