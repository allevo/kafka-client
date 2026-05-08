use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ApiKey, BrokerId, ProduceRequest, ProduceResponse, TopicName};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::client::{CallOptions, Client, NodeTarget, find_partition};
use crate::error::{Error, Result};
use crate::producer::accumulator::{Accumulator, PendingBatch, ReadyBatch};
use crate::producer::{Acks, ProducerConfig};

use super::RecordMetadata;

/// Cap on the per-attempt broker-side wait for a `ProduceRequest`. The
/// `request_timeout` config knob will replace the literal in slice 2.
const PRODUCE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Wire timeout for the per-attempt `Client::send` call. Matches
/// `PRODUCE_REQUEST_TIMEOUT` so the client deadline doesn't fire before
/// the broker's own.
const PRODUCE_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Spawn the long-lived sender task for a [`Producer`]. The task drains
/// the accumulator on every linger tick (and immediately on a wakeup
/// from `ready_rx`), groups frozen batches by leader, and dispatches
/// one `ProduceRequest` per leader.
///
/// Returns the `JoinHandle` so [`Producer::close`] can await graceful
/// shutdown after signalling `shutdown_rx`.
pub(crate) fn spawn_sender(
    client: Client,
    accumulator: Arc<Accumulator>,
    config: Arc<ProducerConfig>,
    ready_rx: mpsc::UnboundedReceiver<()>,
    shutdown_rx: oneshot::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(sender_main(
        client,
        accumulator,
        config,
        ready_rx,
        shutdown_rx,
    ))
}

async fn sender_main(
    client: Client,
    accumulator: Arc<Accumulator>,
    config: Arc<ProducerConfig>,
    mut ready_rx: mpsc::UnboundedReceiver<()>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    // tokio::time::interval requires a non-zero period. A 0ms linger is
    // a legal user choice (see ProducerConfig) so floor at 1ms here —
    // the size-full / wakeup path covers tighter cadences anyway.
    let mut ticker = tokio::time::interval(config.linger.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ready_rx.recv() => {
                dispatch(&client, &accumulator, &config, false).await;
            }
            _ = ticker.tick() => {
                dispatch(&client, &accumulator, &config, false).await;
            }
            _ = &mut shutdown_rx => {
                dispatch(&client, &accumulator, &config, true).await;
                break;
            }
        }
    }
}

/// Drain the accumulator and ship one `ProduceRequest` per leader.
/// `is_final = true` flips the drain to `drain_all` so flush/close
/// don't leave records buffered behind the linger gate.
async fn dispatch(
    client: &Client,
    accumulator: &Accumulator,
    config: &ProducerConfig,
    is_final: bool,
) {
    let drained = if is_final {
        accumulator.drain_all()
    } else {
        accumulator.drain_ready(Instant::now())
    };
    let ready = match drained {
        Ok(r) if r.is_empty() => return,
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "accumulator drain failed");
            return;
        }
    };

    // Group by leader
    let mut by_leader: HashMap<BrokerId, Vec<ReadyBatch>> = HashMap::new();
    for batch in ready {
        let Some(meta) = client.topic_metadata(&batch.topic) else {
            notify_waiters_the_failure(batch.frozen.waiters, || {
                Error::NoBrokerAvailable(format!(
                    "topic '{}' metadata gone",
                    batch.topic.as_str()
                ))
            });
            continue;
        };
        let Some(part) = find_partition(&meta.partitions, batch.partition) else {
            notify_waiters_the_failure(batch.frozen.waiters, || {
                Error::NoBrokerAvailable(format!(
                    "no leader for {}-{}",
                    batch.topic.as_str(),
                    batch.partition.0
                ))
            });
            continue;
        };
        by_leader.entry(part.leader).or_default().push(batch);
    }

    // ProduceRequest v9: 0 = no ack, 1 = leader-only, -1 = full ISR.
    let acks: i16 = match config.acks {
        Acks::None => 0,
        Acks::Leader => 1,
        Acks::All => -1,
    };

    for (leader, batches) in by_leader {
        send_to_leader(client, leader, acks, batches).await;
    }
}

async fn send_to_leader(
    client: &Client,
    leader: BrokerId,
    acks: i16,
    batches: Vec<ReadyBatch>,
) {
    // Split each batch up front: the encoded half is moved into the
    // outgoing `ProduceRequest` (no `Bytes::clone`), the pending half
    // stays here to settle waiters once the response lands.
    let mut topic_groups: HashMap<TopicName, Vec<PartitionProduceData>> = HashMap::new();
    let mut pending_batches: Vec<PendingBatch> = Vec::with_capacity(batches.len());
    for batch in batches {
        let (enc, pending) = batch.split();
        topic_groups.entry(enc.topic).or_default().push(
            PartitionProduceData::default()
                .with_index(enc.partition.0)
                .with_records(Some(enc.encoded)),
        );
        pending_batches.push(pending);
    }

    let topic_data: Vec<TopicProduceData> = topic_groups
        .into_iter()
        .map(|(topic, partition_data)| {
            TopicProduceData::default()
                .with_name(topic)
                .with_partition_data(partition_data)
        })
        .collect();

    let request = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(acks)
        .with_timeout_ms(duration_to_ms(PRODUCE_REQUEST_TIMEOUT))
        .with_topic_data(topic_data);

    if acks == 0 {
        let result = client
            .send_oneway(
                NodeTarget::Broker(leader),
                ApiKey::Produce,
                9,
                request,
                &CallOptions::new().with_timeout(PRODUCE_CALL_TIMEOUT),
            )
            .await;

        match result {
            Ok(()) => {
                for pending in pending_batches {
                    let topic = pending.topic;
                    let partition = pending.partition;
                    for tx in pending.waiters {
                        let _ = tx.send(Ok(RecordMetadata {
                            topic: topic.clone(),
                            partition,
                            // Fire-and-forget: the broker won't reply, so we can't learn the
                            // assigned offset.
                            offset: -1,
                            timestamp: None,
                        }));
                    }
                }
            }
            Err(e) => {
                // The bytes never made it onto the wire; surface this as a
                // real failure rather than synthesising success.
                for pending in pending_batches {
                    notify_waiters_the_failure(pending.waiters, || clone_error(&e));
                }
            }
        }
        return;
    }

    // `with_retries(0)`: a partial-success ProduceResponse (some
    // partitions OK, some not) must not be retransmitted blindly by
    // the client-level retry loop — that would reorder/duplicate the
    // OK partitions. Per-partition retry is the slice-2 work item.
    let result: Result<ProduceResponse> = client
        .send(
            NodeTarget::Broker(leader),
            ApiKey::Produce,
            9,
            request,
            &CallOptions::new()
                .with_retries(0)
                .with_timeout(PRODUCE_CALL_TIMEOUT),
        )
        .await;

    match result {
        Ok(resp) => settle_response(resp, pending_batches),
        Err(e) => {
            for pending in pending_batches {
                notify_waiters_the_failure(pending.waiters, || clone_error(&e));
            }
        }
    }
}

/// Match each (topic, partition) waiter list against the broker's
/// per-partition response and notify it. Missing entries fail with a
/// protocol error — the broker is expected to echo back every
/// partition we sent.
fn settle_response(resp: ProduceResponse, pending: Vec<PendingBatch>) {
    let mut response_index: HashMap<(TopicName, i32), &kafka_protocol::messages::produce_response::PartitionProduceResponse> =
        HashMap::new();
    for tr in &resp.responses {
        for pr in &tr.partition_responses {
            response_index.insert((tr.name.clone(), pr.index), pr);
        }
    }

    for batch in pending {
        let key = (batch.topic.clone(), batch.partition.0);
        let Some(pr) = response_index.get(&key).copied() else {
            notify_waiters_the_failure(batch.waiters, || {
                Error::Protocol(format!(
                    "produce response missing partition {}-{}",
                    batch.topic.as_str(),
                    batch.partition.0
                ))
            });
            continue;
        };

        if pr.error_code == 0 {
            // Broker assigns offsets `base_offset .. base_offset + N`
            // contiguously. log_append_time_ms = -1 means CreateTime
            // mode (broker kept the record's own timestamp), so we
            // surface None; non-negative means LogAppendTime mode.
            let timestamp = if pr.log_append_time_ms == -1 {
                None
            } else {
                Some(pr.log_append_time_ms)
            };
            for (i, tx) in batch.waiters.into_iter().enumerate() {
                let _ = tx.send(Ok(RecordMetadata {
                    topic: batch.topic.clone(),
                    partition: batch.partition,
                    offset: pr.base_offset + i as i64,
                    timestamp,
                }));
            }
        } else {
            // Unknown codes preserve the original i16 instead of
            // collapsing to UnknownServerError, so the waiter sees the
            // exact code the broker returned.
            let response_error =
                ResponseError::try_from_code(pr.error_code).unwrap_or(ResponseError::Unknown(pr.error_code));
            notify_waiters_the_failure(batch.waiters, || Error::Broker {
                error: response_error,
            });
        }
    }
}

/// Notify every waiter with a fresh error built by `make_err`.
fn notify_waiters_the_failure(
    waiters: Vec<oneshot::Sender<Result<RecordMetadata>>>,
    make_err: impl Fn() -> Error,
) {
    for tx in waiters {
        let _ = tx.send(Err(make_err()));
    }
}

/// Best-effort clone of an `Error`. `io::Error` is rebuilt from kind +
/// message because the underlying source isn't clonable; every other
/// variant is a string or `Copy` and clones losslessly.
fn clone_error(e: &Error) -> Error {
    match e {
        Error::Io(io) => Error::Io(std::io::Error::new(io.kind(), io.to_string())),
        Error::Protocol(s) => Error::Protocol(s.clone()),
        Error::Config(s) => Error::Config(s.clone()),
        Error::NoBrokerAvailable(s) => Error::NoBrokerAvailable(s.clone()),
        Error::Authentication(s) => Error::Authentication(s.clone()),
        Error::Broker { error } => Error::Broker { error: *error },
        Error::RequestTimeout(s) => Error::RequestTimeout(s.clone()),
    }
}

fn duration_to_ms(d: Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX)
}
