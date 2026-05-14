//! `BenchProducer` for `samsa` (pure Rust).
//!
//! samsa's `Producer` is a background worker fed over an mpsc channel: it
//! batches messages and flushes them, delivering acks as `Vec<Option<
//! ProduceResponse>>` on a *separate* channel with **no correlation** back
//! to individual `produce` calls. A `ProduceResponse` carries only a
//! `base_offset`, not a per-batch record count, so there is no sound,
//! deadlock-free way to recover an exact per-record send→ack latency from
//! a multi-record batch.
//!
//! To honor the `BenchProducer` contract (per-record send→ack latency,
//! every send resolved before `finalize`) the bench therefore runs samsa
//! with `max_batch_size = 1`: one record per request, one response per
//! record, exact FIFO correlation. The consequences — all surfaced in the
//! methodology footer:
//!
//! - **Client-side batching is disabled.** samsa's throughput column
//!   reflects an unbatched, RTT-bound producer and is *not* comparable to
//!   the batched `this` / rdkafka / rskafka columns. samsa's
//!   `batch_timeout_ms` (its linger analog) is consequently inert.
//! - samsa's worker flushes one batch at a time with no request
//!   pipelining; combined with `max_batch_size = 1` the effective
//!   outstanding-ack depth is ~1 regardless of `--in-flight`.
//! - Sends are serialized through a mutex so the waiter queue stays in
//!   channel order — required for FIFO ack correlation.
//! - Produces only to **partition 0** (samsa's `ProduceMessage` is
//!   per-partition); with `--partitions > 1` it does not spread load.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use samsa::prelude::protocol::ProduceResponse;
use samsa::prelude::{
    BrokerAddress, KafkaCode, ProduceMessage, Producer, ProducerBuilder, TcpConnection,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::bench_producer::BenchProducer;
use crate::producers::parse_host_port;
use crate::workload::{AcksLevel, Workload};

/// Partition this impl produces to — see the module-level asymmetry note.
const PARTITION: i32 = 0;

/// One record per request — see the module docs for why this is forced.
const MAX_BATCH_SIZE: usize = 1;

type AckResult = Result<(), String>;
type WaiterQueue = Arc<Mutex<VecDeque<oneshot::Sender<AckResult>>>>;

pub struct SamsaProducer {
    sender: tokio::sync::mpsc::Sender<ProduceMessage>,
    /// Serializes `send_one` so the waiter queue is pushed in the same
    /// order messages enter samsa's channel — the basis for FIFO ack
    /// correlation in the drain task.
    send_lock: AsyncMutex<()>,
    /// FIFO of pending acks; the drain task pops one per response.
    waiters: WaiterQueue,
    topic: String,
    drain_handle: JoinHandle<()>,
}

impl SamsaProducer {
    pub async fn connect(workload: &Workload) -> anyhow::Result<Self> {
        let (host, port) = parse_host_port(&workload.bootstrap)?;
        let addrs = vec![BrokerAddress { host, port }];

        // samsa's `Error` doesn't implement `std::error::Error`, so `?`
        // can't convert it into `anyhow::Error` directly.
        let mut builder =
            ProducerBuilder::<TcpConnection>::new(addrs, vec![workload.topic.clone()])
                .await
                .map_err(|e| anyhow::anyhow!("samsa producer build: {e}"))?;
        let acks: i16 = match workload.acks {
            AcksLevel::None => 0,
            AcksLevel::Leader => 1,
            AcksLevel::All => -1,
        };
        builder
            .required_acks(acks)
            .max_batch_size(MAX_BATCH_SIZE)
            // Inert at max_batch_size = 1 (a batch is full at one record),
            // but set it to the requested linger anyway for honesty.
            .batch_timeout_ms(workload.linger.as_millis().max(1) as u64);
        let producer = builder.build().await;

        let Producer {
            sender,
            mut receiver,
        } = producer;

        let waiters: WaiterQueue = Arc::new(Mutex::new(VecDeque::new()));
        let waiters_drain = waiters.clone();
        let drain_handle = tokio::spawn(async move {
            // Strict FIFO: max_batch_size = 1 means one response per
            // record, in send order, so popping the front waiter per
            // response correlates exactly.
            while let Some(responses) = receiver.recv().await {
                let result = responses_result(&responses);
                if let Some(tx) = waiters_drain.lock().unwrap().pop_front() {
                    let _ = tx.send(result);
                }
            }
            // Channel closed (producer dropped in `finalize`): the runner
            // has already drained every send, so this normally finds an
            // empty queue — release anything left just in case.
            let mut queue = waiters_drain.lock().unwrap();
            while let Some(tx) = queue.pop_front() {
                let _ = tx.send(Ok(()));
            }
        });

        Ok(Self {
            sender,
            send_lock: AsyncMutex::new(()),
            waiters,
            topic: workload.topic.clone(),
            drain_handle,
        })
    }
}

impl BenchProducer for SamsaProducer {
    async fn send_one(&self, payload: Bytes) -> anyhow::Result<Duration> {
        let msg = ProduceMessage {
            key: None,
            value: Some(payload),
            headers: Vec::new(),
            topic: self.topic.clone(),
            partition_id: PARTITION,
        };
        let (tx, rx) = oneshot::channel();

        let started = Instant::now();
        {
            // Hold the lock across the channel send so the waiter push and
            // the message enqueue stay in the same order across concurrent
            // `send_one` calls.
            let _guard = self.send_lock.lock().await;
            self.waiters.lock().unwrap().push_back(tx);
            self.sender
                .send(msg)
                .await
                .map_err(|_| anyhow::anyhow!("samsa producer worker stopped"))?;
        }
        rx.await
            .map_err(|_| anyhow::anyhow!("samsa drain task dropped before ack"))?
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(started.elapsed())
    }

    async fn finalize(self) -> anyhow::Result<()> {
        let SamsaProducer {
            sender,
            drain_handle,
            ..
        } = self;
        // Dropping the sender ends the worker's input stream; the worker
        // then exits and drops its response sender, closing the drain
        // task's channel.
        drop(sender);
        drain_handle
            .await
            .map_err(|e| anyhow::anyhow!("samsa drain task panicked: {e}"))?;
        Ok(())
    }
}

/// Fold a flush's per-broker responses into a single ack result: `Err` if
/// any partition came back with a non-`None` Kafka error code.
fn responses_result(responses: &[Option<ProduceResponse>]) -> AckResult {
    for resp in responses.iter().flatten() {
        for r in &resp.responses {
            for p in &r.partition_responses {
                if p.error_code != KafkaCode::None {
                    return Err(format!("samsa produce error: {:?}", p.error_code));
                }
            }
        }
    }
    Ok(())
}
