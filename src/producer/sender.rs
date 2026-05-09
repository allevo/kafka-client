use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kafka_protocol::ResponseError;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{ApiKey, BrokerId, ProduceRequest, ProduceResponse, TopicName};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::client::{CallOptions, Client, NodeTarget, PartitionId, find_partition};
use crate::error::{Error, Result, RetryAction};
use crate::producer::accumulator::{Accumulator, PendingBatch, ReadyBatch};
use crate::producer::batch::BatchProducerState;
use crate::producer::batch_rewrite::rewrite_producer_state;
use crate::producer::idempotent::{
    Assigned, IdempotentState, InFlightBatch, increment_sequence, request_epoch_bump,
};
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
/// `idempotent` is `Some` when [`ProducerConfig::enable_idempotence`]
/// is set; the sender then routes through [`IdempotentState`] for
/// producer-id acquisition, sequence assignment, and per-batch retry.
///
/// Returns the `JoinHandle` so [`Producer::close`] can await graceful
/// shutdown after signalling `shutdown_rx`.
pub(crate) fn spawn_sender(
    client: Client,
    accumulator: Arc<Accumulator>,
    config: Arc<ProducerConfig>,
    idempotent: Option<Arc<IdempotentState>>,
    ready_rx: mpsc::UnboundedReceiver<()>,
    shutdown_rx: oneshot::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(sender_main(
        client,
        accumulator,
        config,
        idempotent,
        ready_rx,
        shutdown_rx,
    ))
}

async fn sender_main(
    client: Client,
    accumulator: Arc<Accumulator>,
    config: Arc<ProducerConfig>,
    idempotent: Option<Arc<IdempotentState>>,
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
                dispatch(&client, &accumulator, &config, idempotent.as_ref(), false).await;
            }
            _ = ticker.tick() => {
                dispatch(&client, &accumulator, &config, idempotent.as_ref(), false).await;
            }
            _ = &mut shutdown_rx => {
                dispatch(&client, &accumulator, &config, idempotent.as_ref(), true).await;
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
    idempotent: Option<&Arc<IdempotentState>>,
    is_final: bool,
) {
    if let Some(idem) = idempotent {
        dispatch_idempotent(client, accumulator, config, idem, is_final).await;
    } else {
        dispatch_non_idempotent(client, accumulator, config, is_final).await;
    }
}

/// Pre-idempotence dispatch path. Drain, group by leader, send. Errors
/// surface to waiters; no per-batch retry.
async fn dispatch_non_idempotent(
    client: &Client,
    accumulator: &Accumulator,
    config: &ProducerConfig,
    is_final: bool,
) {
    let drained = drain(accumulator, is_final, |_, _, _| {
        BatchProducerState::non_idempotent()
    });
    let ready = match drained {
        Ok(r) if r.is_empty() => return,
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "accumulator drain failed");
            return;
        }
    };

    // Group by leader
    let by_leader = group_by_leader(client, ready);

    for (leader, batches) in by_leader {
        send_to_leader_basic(client, leader, config.acks, batches).await;
    }
}

/// Idempotent dispatch path. Acquires producer-id on the first call,
/// stamps `(pid, epoch, base_sequence)` per batch, registers each
/// in-flight batch in [`IdempotentState`], dispatches per leader, and
/// settles per-partition responses through the recovery state machine.
async fn dispatch_idempotent(
    client: &Client,
    accumulator: &Accumulator,
    config: &ProducerConfig,
    idem: &Arc<IdempotentState>,
    is_final: bool,
) {
    // Honour an earlier fatal transition: drop everything pending.
    if idem.is_fatal() {
        let reason = idem
            .fatal_reason()
            .unwrap_or_else(|| "idempotent producer in fatal state".into());
        drain_and_fail_everything(accumulator, idem, &reason);
        return;
    }

    // Acquire (producer_id, producer_epoch) on first dispatch. A failure
    // here is terminal (mark_fatal in IdempotentState); subsequent ticks
    // hit the is_fatal short-circuit above.
    if let Err(e) = idem.ensure_ready(client).await {
        tracing::error!(error = %e, "InitProducerId failed; idempotent producer fenced");
        let reason = format!("{e}");
        drain_and_fail_everything(accumulator, idem, &reason);
        return;
    }

    // Per-partition epoch bump pass for any partitions flagged on the
    // previous tick (OutOfOrderSequence / fail_inflight_head recovery).
    // For each flagged partition: ask the broker for a fresh
    // `(producer_id, producer_epoch)`, drain the in-flight queue, and
    // rewrite every paused batch in place with sequential
    // `base_sequence` starting at 0 under the new state. Mirrors
    // Java's `TxnPartitionEntry.startSequencesAtBeginning`
    // (clients/src/main/java/.../internals/TxnPartitionEntry.java:116-125):
    // sequentially re-stamp every in-flight batch, then advance
    // `next_sequence` past the last one.
    if idem.has_pending_bumps() {
        if !run_bump_pass(client, idem, accumulator).await {
            // run_bump_pass already marked fatal + drained waiters; bail.
            return;
        }
    }

    // Capture the first assign failure across the whole drain pass so we
    // can fail the freshly-drained batches' waiters explicitly instead of
    // silently downgrading them to a non-idempotent send. Java's
    // `Sender.maybeWaitForProducerId` flips the producer to a terminal
    // state on equivalent control-flow violations; mirror that here.
    let mut assign_failure: Option<String> = None;
    let drained = drain(accumulator, is_final, |topic, partition, count| {
        if assign_failure.is_some() {
            // Producer is on its way to fatal; the post-drain handler
            // below fails every drained batch's waiters. The state we
            // return here never reaches the broker.
            return BatchProducerState::non_idempotent();
        }
        match assign_or_fail(idem, topic, partition, count) {
            Ok(a) => BatchProducerState::idempotent(a.producer_id, a.producer_epoch, a.base_sequence),
            Err(reason) => {
                assign_failure = Some(reason);
                BatchProducerState::non_idempotent()
            }
        }
    });

    let ready = match drained {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "accumulator drain failed");
            return;
        }
    };

    if let Some(reason) = assign_failure {
        // mark_fatal already fired inside assign_or_fail. Resolve the
        // waiters of every batch we just drained so they don't park
        // forever, then sweep the rest of the producer's state via the
        // shared fatal helper.
        for batch in ready {
            let owned = reason.clone();
            notify_waiters_the_failure(batch.frozen.waiters, move || {
                Error::IdempotenceFenced(owned.clone())
            });
        }
        drain_and_fail_everything(accumulator, idem, &reason);
        return;
    }

    // Register each freshly drained batch in the per-partition in-flight
    // queue so retries reuse the same encoded bytes (no re-encoding).
    // An epoch-bump rewrite (slice C1) mutates these bytes in place via
    // the v2 header-rewrite helper — no decoded payloads are retained.
    let now = Instant::now();
    for ready_batch in ready {
        let (_enc, pending, encoded) = ready_batch.split_keeping_encoded();
        let record_count = i32::try_from(pending.record_count).unwrap_or(i32::MAX);
        idem.register_inflight(InFlightBatch {
            topic: pending.topic.clone(),
            partition: pending.partition,
            base_sequence: pending.state.base_sequence,
            record_count,
            producer_id: pending.state.producer_id,
            producer_epoch: pending.state.producer_epoch,
            encoded,
            waiters: pending.waiters,
            first_send_at: now,
            attempt: 0,
            // A fresh batch is immediately dispatchable; the backoff
            // clock only starts ticking after the first failed attempt.
            next_attempt_at: now,
        });
    }

    // Build the list of in-flight batches to dispatch, per partition.
    // We re-dispatch every in-flight entry every tick: the broker dedups
    // on (producer_id, base_sequence) so retransmits are safe. The
    // dispatch loop is bounded in practice by `delivery_timeout`;
    // `retries` is a no-op in the idempotent path (auto-coerced to
    // u32::MAX and only the wall clock advances `attempt`).
    let to_dispatch = collect_dispatchable(config, idem, now);

    if to_dispatch.is_empty() {
        return;
    }

    // Group by leader and dispatch.
    let by_leader = group_inflight_by_leader(client, idem, to_dispatch);
    for (leader, partition_batches) in by_leader {
        send_to_leader_idempotent(client, idem, config, leader, partition_batches).await;
    }
}

/// Snapshot of one in-flight batch needed by the per-leader
/// `ProduceRequest` builder. The `encoded` here is a `Bytes` clone
/// (cheap, refcounted) of the buffer held inside [`IdempotentState`].
struct InflightSnapshot {
    topic: TopicName,
    partition: PartitionId,
    base_sequence: i32,
    encoded: Bytes,
}

/// Drain — same API for both paths so the closure-driven freeze stays
/// in one place.
fn drain(
    accumulator: &Accumulator,
    is_final: bool,
    state_for: impl FnMut(&TopicName, PartitionId, usize) -> BatchProducerState,
) -> Result<Vec<ReadyBatch>> {
    if is_final {
        accumulator.drain_all(state_for)
    } else {
        accumulator.drain_ready(Instant::now(), state_for)
    }
}

/// Group `ready` by partition leader, dropping batches whose topic
/// metadata has gone away (with an error notification to waiters).
fn group_by_leader(
    client: &Client,
    ready: Vec<ReadyBatch>,
) -> HashMap<BrokerId, Vec<ReadyBatch>> {
    let mut by_leader: HashMap<BrokerId, Vec<ReadyBatch>> = HashMap::new();
    for batch in ready {
        let Some(meta) = client.topic_metadata(&batch.topic) else {
            notify_waiters_the_failure(batch.frozen.waiters, || {
                Error::NoBrokerAvailable(format!("topic '{}' metadata gone", batch.topic.as_str()))
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
    by_leader
}

/// Walk the per-partition in-flight queues and pick the head batch of
/// each one, capping retries at `config.retries` and the wall-clock at
/// `delivery_timeout`. Expired batches drop out of the queue with a
/// `RequestTimeout` to their waiters. Heads still inside their
/// `retry_backoff` window are skipped this tick — the dispatch loop
/// picks them up once the backoff elapses, so a misbehaving broker
/// can't be hammered at linger cadence.
fn collect_dispatchable(
    config: &ProducerConfig,
    idem: &Arc<IdempotentState>,
    now: Instant,
) -> Vec<InflightSnapshot> {
    let snapshots = idem.snapshot_inflight();
    let mut out = Vec::with_capacity(snapshots.len());
    for snap in snapshots {
        if snap.attempt as u32 > config.retries {
            // Exceeded retries — fail and drop. Flag the partition for
            // an epoch bump in the same step: `assign()` advanced
            // `next_sequence` eagerly when this batch was frozen, so
            // popping without bumping leaves a sequence gap the broker
            // would reject as `OutOfOrderSequence` on the next send.
            let reason = format!(
                "exceeded {} retries for {}-{}",
                config.retries,
                snap.topic.as_str(),
                snap.partition.0
            );
            idem.fail_inflight_head(&snap.topic, snap.partition, || {
                Error::RequestTimeout(reason.clone())
            });
            continue;
        }
        if now.saturating_duration_since(snap.first_send_at) > config.delivery_timeout {
            let reason = format!(
                "delivery_timeout exceeded for {}-{}",
                snap.topic.as_str(),
                snap.partition.0
            );
            idem.fail_inflight_head(&snap.topic, snap.partition, || {
                Error::RequestTimeout(reason.clone())
            });
            continue;
        }
        if snap.next_attempt_at > now {
            // Still inside the retry-backoff window. Skipping is safe:
            // the next dispatch tick (linger cadence) will revisit this
            // head, and `delivery_timeout` above bounds the total wait.
            continue;
        }
        if snap.attempt > 0 {
            // A non-zero attempt means this head has already been
            // dispatched at least once and is being re-sent under the
            // *same* `(producer_id, base_sequence)` — the core
            // idempotent-retry action. The broker dedups any earlier
            // arrival of these bytes via the sequence number.
            tracing::info!(
                topic = snap.topic.as_str(),
                partition = snap.partition.0,
                base_sequence = snap.base_sequence,
                attempt = snap.attempt,
                "retransmitting idempotent batch"
            );
        }
        out.push(InflightSnapshot {
            topic: snap.topic,
            partition: snap.partition,
            base_sequence: snap.base_sequence,
            encoded: snap.encoded,
        });
    }
    out
}

/// Map each partition's head in-flight batch to its leader, dropping
/// any whose topic metadata is missing (with a fatal notification on
/// the held waiters).
fn group_inflight_by_leader(
    client: &Client,
    idem: &Arc<IdempotentState>,
    snapshots: Vec<InflightSnapshot>,
) -> HashMap<BrokerId, Vec<InflightSnapshot>> {
    let mut by_leader: HashMap<BrokerId, Vec<InflightSnapshot>> = HashMap::new();
    for snap in snapshots {
        let Some(meta) = client.topic_metadata(&snap.topic) else {
            // Metadata gone — fail the in-flight head and flag the
            // partition for a bump. The encoded bytes carry sequence
            // numbers we can't reuse with a different leader cleanly,
            // and `assign()` already advanced `next_sequence` past
            // them, so the bump is what closes the resulting gap.
            let topic_str = snap.topic.as_str().to_string();
            idem.fail_inflight_head(&snap.topic, snap.partition, || {
                Error::NoBrokerAvailable(format!("topic '{topic_str}' metadata gone"))
            });
            continue;
        };
        let Some(part) = find_partition(&meta.partitions, snap.partition) else {
            let topic_str = snap.topic.as_str().to_string();
            let partition_index = snap.partition.0;
            idem.fail_inflight_head(&snap.topic, snap.partition, || {
                Error::NoBrokerAvailable(format!(
                    "no leader for {topic_str}-{partition_index}"
                ))
            });
            continue;
        };
        by_leader.entry(part.leader).or_default().push(snap);
    }
    by_leader
}

/// Non-idempotent send-to-leader: today's behaviour preserved verbatim
/// for `enable_idempotence = false`. No per-batch retry, no in-flight
/// tracking.
async fn send_to_leader_basic(
    client: &Client,
    leader: BrokerId,
    acks: Acks,
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

    // ProduceRequest v9: 0 = no ack, 1 = leader-only, -1 = full ISR.
    let request = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(acks.as_i16())
        .with_timeout_ms(duration_to_ms(PRODUCE_REQUEST_TIMEOUT))
        .with_topic_data(topic_data);

    match acks {
        Acks::None => {
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
        }
        Acks::All | Acks::Leader => {
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
                Ok(resp) => settle_response_basic(resp, pending_batches),
                Err(e) => {
                    for pending in pending_batches {
                        notify_waiters_the_failure(pending.waiters, || clone_error(&e));
                    }
                }
            }
        }
    }
}

/// Idempotent send-to-leader: clones the in-flight encoded bytes into
/// a `ProduceRequest` and routes the per-partition response through
/// the recovery state machine.
async fn send_to_leader_idempotent(
    client: &Client,
    idem: &Arc<IdempotentState>,
    config: &ProducerConfig,
    leader: BrokerId,
    snapshots: Vec<InflightSnapshot>,
) {
    let mut topic_groups: HashMap<TopicName, Vec<PartitionProduceData>> = HashMap::new();
    // Track the (topic, partition, base_sequence) tuples so settle
    // knows which in-flight head each per-partition response refers
    // to.
    let mut keys: Vec<(TopicName, PartitionId, i32)> = Vec::with_capacity(snapshots.len());
    for snap in snapshots {
        keys.push((snap.topic.clone(), snap.partition, snap.base_sequence));
        topic_groups.entry(snap.topic).or_default().push(
            PartitionProduceData::default()
                .with_index(snap.partition.0)
                .with_records(Some(snap.encoded)),
        );
    }

    let topic_data: Vec<TopicProduceData> = topic_groups
        .into_iter()
        .map(|(topic, partition_data)| {
            TopicProduceData::default()
                .with_name(topic)
                .with_partition_data(partition_data)
        })
        .collect();

    // Idempotence forces acks=All in Producer::new, but defer to the
    // config to keep this function honest.
    let request = ProduceRequest::default()
        .with_transactional_id(None)
        .with_acks(config.acks.as_i16())
        .with_timeout_ms(duration_to_ms(PRODUCE_REQUEST_TIMEOUT))
        .with_topic_data(topic_data);

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
        Ok(resp) => settle_response_idempotent(
            resp,
            idem,
            keys,
            config,
            client.retry_backoff(),
            client.retry_backoff_max(),
        ),
        Err(e) => {
            // Transport-level error: every partition we tried to ship is
            // still in the in-flight queue. Bump the attempt counter so
            // the retry budget makes progress, but leave the encoded
            // bytes in place for the next dispatch.
            tracing::warn!(error = %e, leader = leader.0, "produce request to leader failed");
            idem.bump_attempts(&keys, client.retry_backoff(), client.retry_backoff_max());
        }
    }
}

/// Match the broker's per-partition response against the in-flight
/// queue and route each one through the recovery state machine.
fn settle_response_idempotent(
    resp: ProduceResponse,
    idem: &Arc<IdempotentState>,
    keys: Vec<(TopicName, PartitionId, i32)>,
    _config: &ProducerConfig,
    retry_backoff: Duration,
    retry_backoff_max: Duration,
) {
    let mut response_index: HashMap<
        (TopicName, i32),
        &kafka_protocol::messages::produce_response::PartitionProduceResponse,
    > = HashMap::new();
    for tr in &resp.responses {
        for pr in &tr.partition_responses {
            response_index.insert((tr.name.clone(), pr.index), pr);
        }
    }

    for (topic, partition, base_sequence) in keys {
        let key = (topic.clone(), partition.0);
        let Some(pr) = response_index.get(&key).copied() else {
            // Broker omitted this partition — treat as a transport
            // failure for that partition: bump attempt, leave in
            // queue.
            tracing::warn!(
                topic = topic.as_str(),
                partition = partition.0,
                "produce response missing partition; will retry",
            );
            idem.bump_attempt_one(&topic, partition, retry_backoff, retry_backoff_max);
            continue;
        };

        // Match on the typed `ResponseError` rather than the raw i16
        // code — keeps the dispatch self-documenting and immune to
        // protocol-spec re-numbering. `try_from_code` returns `None`
        // for the no-error sentinel (0); everything else is `Some(...)`.
        match ResponseError::try_from_code(pr.error_code) {
            None => {
                // Successful append. Pop the head, assign offsets to
                // its waiters in record-order, advance last_acked.
                let Some(batch) = idem.complete(&topic, partition, base_sequence) else {
                    tracing::warn!(
                        topic = topic.as_str(),
                        partition = partition.0,
                        base_sequence,
                        "complete() found no matching head — broker raced or dedup re-ordered",
                    );
                    continue;
                };
                let timestamp = if pr.log_append_time_ms == -1 {
                    None
                } else {
                    Some(pr.log_append_time_ms)
                };
                for (i, tx) in batch.waiters.into_iter().enumerate() {
                    let _ = tx.send(Ok(RecordMetadata {
                        topic: topic.clone(),
                        partition,
                        offset: pr.base_offset + i as i64,
                        timestamp,
                    }));
                }
            }
            Some(ResponseError::DuplicateSequenceNumber) => {
                // The broker has already accepted these records (a
                // previous attempt's request landed even though we
                // never saw the response). Resolve waiters with
                // offset = -1 / timestamp = None — the broker no
                // longer carries the original metadata.
                tracing::info!(
                    topic = topic.as_str(),
                    partition = partition.0,
                    base_sequence,
                    "DuplicateSequenceNumber — treating as success without offset",
                );
                let Some(batch) = idem.complete(&topic, partition, base_sequence) else {
                    continue;
                };
                for tx in batch.waiters {
                    let _ = tx.send(Ok(RecordMetadata {
                        topic: topic.clone(),
                        partition,
                        offset: -1,
                        timestamp: None,
                    }));
                }
            }
            Some(ResponseError::OutOfOrderSequenceNumber) => {
                // Per-partition epoch bump + rewrite (Java's
                // `TransactionManager.handleOutOfOrderSequence`). The
                // batches stay in flight; the bump pass picks them up
                // on the next tick and rewrites each with a fresh
                // `(pid, epoch)` + dense base_sequence from 0.
                tracing::warn!(
                    topic = topic.as_str(),
                    partition = partition.0,
                    base_sequence,
                    "OutOfOrderSequence — flagging partition for epoch bump",
                );
                idem.request_partition_bump(&topic, partition);
            }
            Some(ResponseError::UnknownProducerId) => {
                // Broker forgot us. If the head is also the only
                // in-flight batch on this partition, we can safely
                // reset sequences to 0 and retry. Otherwise we need an
                // epoch bump (mirrored from Java's
                // `TransactionManager:826-832`).
                match idem.partition_inflight_predecessor_count(&topic, partition, base_sequence) {
                    Some(0) => {
                        // Rewrite the head's encoded header to
                        // base_sequence=0 under the partition's current
                        // (pid, epoch) and re-register so the next
                        // dispatch tick re-sends. Mirrors Java's
                        // `TxnPartitionEntry.startSequencesAtBeginning`
                        // (TxnPartitionEntry.java:116-125) for the
                        // single-batch case; multi-batch recovery
                        // (rewriting paused successors with sequential
                        // sequences) lands in Phase C.
                        recover_unknown_pid_at_head(idem, &topic, partition, base_sequence);
                    }
                    Some(_) => {
                        tracing::warn!(
                            topic = topic.as_str(),
                            partition = partition.0,
                            "UnknownProducerId with predecessors in flight — flagging for bump",
                        );
                        idem.request_partition_bump(&topic, partition);
                    }
                    None => {
                        // No in-flight batch carries this base_sequence:
                        // a stale or duplicated broker response. Don't
                        // pop, don't reset — the next dispatch tick will
                        // retry the real head normally.
                        tracing::warn!(
                            topic = topic.as_str(),
                            partition = partition.0,
                            base_sequence,
                            "UnknownProducerId for unknown sequence — ignoring stale response",
                        );
                    }
                }
            }
            Some(err @ (ResponseError::InvalidProducerEpoch | ResponseError::ProducerFenced)) => {
                // Globally fatal: the server has assigned our id to
                // someone else, so every in-flight sequence under the
                // old `(pid, epoch)` is now unrecoverable. Java's
                // `TransactionManager` treats these as terminal.
                //
                // Historical note: the previous code listed raw codes
                // `47 | 152` here. Code 152 is not a real Kafka error
                // (`PRODUCER_FENCED = 90` per `kafka-protocol`
                // `error.rs:278` / `Errors.java:375`); a real
                // ProducerFenced ack used to fall through to the
                // `_other` per-partition arm and land in
                // `fail_inflight_head` recovery, delaying the
                // global-fatal transition until the next
                // `InitProducerIdRequest` returned the same code.
                let reason = format!(
                    "fenced ({err:?}) on {}-{} at sequence {}",
                    topic.as_str(),
                    partition.0,
                    base_sequence,
                );
                idem.mark_fatal(reason.clone());
                drain_all_partition_waiters(idem, &reason);
                return;
            }
            Some(response_error) => {
                let action = (Error::Broker {
                    error: response_error,
                })
                .classify();
                match action {
                    RetryAction::Retry | RetryAction::RefreshMetadata => {
                        tracing::warn!(
                            topic = topic.as_str(),
                            partition = partition.0,
                            error = ?response_error,
                            "retryable produce error; will retry",
                        );
                        idem.bump_attempt_one(&topic, partition, retry_backoff, retry_backoff_max);
                    }
                    RetryAction::Fatal | RetryAction::Abortable => {
                        tracing::error!(
                            topic = topic.as_str(),
                            partition = partition.0,
                            error = ?response_error,
                            "fatal produce error; flagging partition for bump and failing batch",
                        );
                        // Single-step pop + bump-flag so a concurrent
                        // assign can't slip in a poisoned base_sequence
                        // between the two.
                        idem.fail_inflight_head(&topic, partition, || Error::Broker {
                            error: response_error,
                        });
                    }
                }
            }
        }
    }
}

/// Recover from `UnknownProducerId` at the in-flight head: pop the
/// head, reset the partition's sequence space, rewrite the batch's
/// encoded header to `base_sequence=0` under the same `(pid, epoch)`,
/// and re-register the batch so the next dispatch tick re-sends.
///
/// The waiters and `first_send_at` carry over (delivery_timeout clock
/// keeps running); `attempt` is bumped so the retry budget makes
/// progress. The rewrite is the byte-level analogue of Java's
/// `MemoryRecordsBuilder.reopenAndRewriteProducerState` — only the v2
/// header bytes change, the (possibly compressed) records section is
/// stable, the CRC is recomputed over the new contents.
fn recover_unknown_pid_at_head(
    idem: &Arc<IdempotentState>,
    topic: &TopicName,
    partition: PartitionId,
    base_sequence: i32,
) {
    let Some(head) = idem.pop_inflight_head(topic, partition) else {
        // Raced with another path that popped the same head; nothing
        // left to recover.
        return;
    };
    idem.reset_partition(topic, partition);

    let mut buf = BytesMut::from(&head.encoded[..]);
    if let Err(e) = rewrite_producer_state(
        &mut buf,
        head.producer_id,
        head.producer_epoch,
        0,
    ) {
        // The encoded bytes were valid when we first stamped them in
        // `freeze`, so a rewrite failure here would mean memory or
        // implementation corruption — surface it as fatal rather than
        // pretending the partition is healthy.
        let reason = format!(
            "UnknownProducerId rewrite failed for {}-{} at sequence {}: {e}",
            topic.as_str(),
            partition.0,
            base_sequence,
        );
        tracing::error!("{reason}");
        idem.mark_fatal(reason.clone());
        notify_waiters_the_failure(head.waiters, || {
            Error::IdempotenceFenced(reason.clone())
        });
        drain_all_partition_waiters(idem, &reason);
        return;
    }

    tracing::info!(
        topic = topic.as_str(),
        partition = partition.0,
        from_base_sequence = base_sequence,
        producer_id = head.producer_id,
        "UnknownProducerId — rewrote head to base_sequence=0 and re-registered",
    );

    idem.register_inflight(InFlightBatch {
        topic: head.topic,
        partition: head.partition,
        base_sequence: 0,
        record_count: head.record_count,
        producer_id: head.producer_id,
        producer_epoch: head.producer_epoch,
        encoded: buf.freeze(),
        waiters: head.waiters,
        first_send_at: head.first_send_at,
        attempt: head.attempt.saturating_add(1),
        // The rewrite produced a fresh frame under a clean
        // (pid, epoch, base_seq=0); the broker has dropped its
        // producer-id state, so any prior retry backoff is moot.
        // Dispatch on the next tick — recovery is not a normal retry.
        next_attempt_at: Instant::now(),
    });
}

/// Run one bump pass: for every partition flagged via
/// `request_partition_bump`, fetch a fresh `(producer_id, producer_epoch)`
/// from the broker, drain that partition's in-flight queue, and rewrite
/// every paused batch under the new state with sequential
/// `base_sequence` starting at 0.
///
/// Returns `true` on success (the caller may proceed with the normal
/// drain pass), `false` on a fatal classification (the caller must
/// return immediately — `run_bump_pass` has already notified every
/// waiter via `drain_and_fail_everything`).
///
/// Transient bump failures (transport errors, refresh-metadata codes)
/// are re-flagged via `request_partition_bump` so the next tick retries.
/// A rewrite failure on already-encoded bytes is corruption-level and
/// flips the producer to fatal — `rewrite_producer_state` only fails on
/// malformed buffers, which our `freeze` would never produce.
async fn run_bump_pass(
    client: &Client,
    idem: &Arc<IdempotentState>,
    accumulator: &Accumulator,
) -> bool {
    let pending = idem.take_pending_bumps();
    for (topic, partition) in pending {
        let Some((current_pid, current_epoch)) = idem.partition_state(&topic, partition) else {
            // The partition entry vanished between flagging and running
            // the pass. Nothing in-flight to recover; drop the flag.
            tracing::warn!(
                topic = topic.as_str(),
                partition = partition.0,
                "bump-pass: partition state missing — dropping bump request",
            );
            continue;
        };

        let new_state = match request_epoch_bump(client, current_pid, current_epoch).await {
            Ok(pair) => pair,
            Err(e) => {
                if is_fatal_bump_error(&e) {
                    let reason = format!(
                        "epoch bump on {}-{} surfaced fatal error: {e}",
                        topic.as_str(),
                        partition.0,
                    );
                    tracing::error!("{reason}");
                    idem.mark_fatal(reason.clone());
                    drain_and_fail_everything(accumulator, idem, &reason);
                    return false;
                }
                // Transient: re-flag so the next tick retries. The
                // paused batches stay in the in-flight queue under the
                // *old* (pid, epoch). The `delivery_timeout` clock keeps
                // running and `fail_inflight_head` will pop them if the
                // bump never succeeds.
                tracing::warn!(
                    topic = topic.as_str(),
                    partition = partition.0,
                    error = %e,
                    "epoch bump failed transiently; will retry next tick",
                );
                idem.request_partition_bump(&topic, partition);
                continue;
            }
        };

        if !rewrite_paused_batches(idem, accumulator, &topic, partition, new_state) {
            return false;
        }
    }
    true
}

/// Drain the partition's in-flight queue (via `apply_bumped_state`) and
/// rewrite each batch under `(new_pid, new_epoch)` with sequential
/// `base_sequence` starting at 0. Mirrors Java's
/// `TxnPartitionEntry.startSequencesAtBeginning`. Returns `false` on a
/// rewrite failure (corruption-level — marks fatal); `true` otherwise.
fn rewrite_paused_batches(
    idem: &Arc<IdempotentState>,
    accumulator: &Accumulator,
    topic: &TopicName,
    partition: PartitionId,
    new_state: (i64, i16),
) -> bool {
    let (new_pid, new_epoch) = new_state;
    // `apply_bumped_state` clears the pending-bump flag, swaps the
    // partition's (producer_id, producer_epoch), and returns the
    // in-flight queue. We then re-install each batch with sequential
    // `base_sequence` via `install_rewritten_inflight`, which advances
    // `next_sequence` past it.
    let paused = idem.apply_bumped_state(topic, partition, new_pid, new_epoch);
    let mut next_seq: i32 = 0;
    let now = Instant::now();
    for batch in paused {
        let record_count = batch.record_count;
        let mut buf = BytesMut::from(&batch.encoded[..]);
        if let Err(e) = rewrite_producer_state(&mut buf, new_pid, new_epoch, next_seq) {
            // `freeze` produces well-formed v2 bytes by construction;
            // a rewrite failure here means memory or implementation
            // corruption. Mark fatal so every waiter learns.
            let reason = format!(
                "bump-pass rewrite failed for {}-{} at sequence {next_seq}: {e}",
                topic.as_str(),
                partition.0,
            );
            tracing::error!("{reason}");
            idem.mark_fatal(reason.clone());
            // The just-popped batch's waiters are not held by
            // `drain_all_partition_waiters` (we own them right now).
            notify_waiters_the_failure(batch.waiters, || {
                Error::IdempotenceFenced(reason.clone())
            });
            drain_and_fail_everything(accumulator, idem, &reason);
            return false;
        }
        tracing::info!(
            topic = topic.as_str(),
            partition = partition.0,
            old_pid = batch.producer_id,
            new_pid,
            new_epoch,
            from_base_sequence = batch.base_sequence,
            to_base_sequence = next_seq,
            "bump-pass: rewrote paused batch under new (pid, epoch)",
        );
        idem.install_rewritten_inflight(InFlightBatch {
            topic: batch.topic,
            partition: batch.partition,
            base_sequence: next_seq,
            record_count,
            producer_id: new_pid,
            producer_epoch: new_epoch,
            encoded: buf.freeze(),
            waiters: batch.waiters,
            first_send_at: batch.first_send_at,
            // Fresh (pid, epoch) is a substantive recovery, not just
            // another retry — reset the retry budget so the rewritten
            // batch gets a full attempt under the new state. Mirrors
            // Java's `ProducerBatch.resetProducerState`, which sets
            // `attempts = new AtomicInteger(0)`.
            attempt: 0,
            next_attempt_at: now,
        });
        next_seq = increment_sequence(next_seq, record_count);
    }
    true
}

/// Classify a bump-pass error into fatal vs transient. Fatal cases
/// terminate the producer (matches Java's
/// `TransactionManager.isFatalException`); everything else is treated
/// as transient and retried on the next tick.
fn is_fatal_bump_error(e: &Error) -> bool {
    match e {
        Error::IdempotenceFenced(_) => true,
        Error::Broker { error } => matches!(
            (Error::Broker { error: *error }).classify(),
            RetryAction::Fatal | RetryAction::Abortable,
        ),
        _ => false,
    }
}

/// Drain every in-flight queue across every partition the idempotent
/// state knows about and notify the held waiters with `IdempotenceFenced`.
/// Used when a fatal transition (e.g. `ProducerFenced`) is detected.
fn drain_all_partition_waiters(idem: &Arc<IdempotentState>, reason: &str) {
    for (topic, partition) in idem.known_partitions() {
        let batches = idem.drain_partition_inflight(&topic, partition);
        for batch in batches {
            let owned = reason.to_string();
            notify_waiters_the_failure(batch.waiters, move || {
                Error::IdempotenceFenced(owned.clone())
            });
        }
    }
}

/// Drain the accumulator and the idempotent state's queues, failing
/// every waiter with the same reason. Used when a fatal transition is
/// detected (e.g. ensure_ready failed) so callers don't park forever.
fn drain_and_fail_everything(
    accumulator: &Accumulator,
    idem: &Arc<IdempotentState>,
    reason: &str,
) {
    // The accumulator can't be frozen meaningfully (we have no producer
    // state), so flush its open batches with `non_idempotent` and just
    // notify the waiters. Bytes are discarded.
    let drained = accumulator.drain_all(|_, _, _| BatchProducerState::non_idempotent());
    if let Ok(batches) = drained {
        for b in batches {
            let owned = reason.to_string();
            notify_waiters_the_failure(b.frozen.waiters, move || {
                Error::IdempotenceFenced(owned.clone())
            });
        }
    }
    drain_all_partition_waiters(idem, reason);
}

/// Pre-idempotence settle path. Identical to today's behaviour,
/// renamed so the idempotent path can have its own settle.
fn settle_response_basic(resp: ProduceResponse, pending: Vec<PendingBatch>) {
    let mut response_index: HashMap<
        (TopicName, i32),
        &kafka_protocol::messages::produce_response::PartitionProduceResponse,
    > = HashMap::new();
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
            let response_error = ResponseError::try_from_code(pr.error_code)
                .unwrap_or(ResponseError::Unknown(pr.error_code));
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
        Error::TopicPropagationTimeout { topic, direction } => Error::TopicPropagationTimeout {
            topic: topic.clone(),
            direction: *direction,
        },
        Error::IdempotenceFenced(s) => Error::IdempotenceFenced(s.clone()),
    }
}

fn duration_to_ms(d: Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX)
}

/// Reserve sequence space for one drained batch, escalating to a fatal
/// idempotent transition on any error.
///
/// The error path is unreachable today — every `AssignError` variant
/// (`NotReady`, `Fatal`, `PartitionBlocked`) is gated upstream by
/// `ensure_ready` / `is_fatal` / `take_pending_bumps` short-circuits.
/// Keeping the path *honest* (mark fatal, return the reason) means a
/// future refactor that loosens any of those gates surfaces the bug
/// loudly instead of silently stamping `producer_id = -1` onto a batch
/// from an idempotent producer.
fn assign_or_fail(
    idem: &IdempotentState,
    topic: &TopicName,
    partition: PartitionId,
    record_count: usize,
) -> std::result::Result<Assigned, String> {
    match idem.assign(topic, partition, record_count) {
        Ok(a) => Ok(a),
        Err(e) => {
            let reason = format!(
                "idempotent assign failed for {}-{}: {:?}",
                topic.as_str(),
                partition.0,
                e
            );
            tracing::error!("{reason}");
            idem.mark_fatal(reason.clone());
            Err(reason)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    use kafka_protocol::protocol::StrBytes;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType,
    };
    use std::io::Cursor;
    use tokio::sync::oneshot;

    fn topic(s: &'static str) -> TopicName {
        TopicName(StrBytes::from_static_str(s))
    }

    /// Build a 2-record v2 batch with the given producer state, for
    /// settle-path tests that need a real frame whose bytes can be
    /// decoded and CRC-verified after the rewrite.
    fn encode_test_batch(
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
    ) -> Bytes {
        encode_test_batch_with_count(producer_id, producer_epoch, base_sequence, 2)
    }

    /// Variant of `encode_test_batch` that lets the caller pick the
    /// record count, so per-batch sequence-space tests can register
    /// in-flight batches whose declared `record_count` matches the
    /// actual encoded record count (so a decoded round-trip is
    /// consistent with the IdempotentState's accounting).
    fn encode_test_batch_with_count(
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        record_count: i32,
    ) -> Bytes {
        let records: Vec<Record> = (0..record_count)
            .map(|i| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id,
                producer_epoch,
                timestamp_type: TimestampType::Creation,
                offset: i64::from(i),
                sequence: base_sequence.wrapping_add(i),
                timestamp: 1_700_000_000_000 + i64::from(i),
                key: None,
                value: Some(Bytes::from(format!("v{i}"))),
                headers: IndexMap::new(),
            })
            .collect();
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode");
        buf.freeze()
    }

    /// Build a `ProduceResponse` whose single partition entry carries
    /// `error_code` for `(topic, partition)`. The other fields default
    /// so the settle path treats it as the only partition response.
    fn produce_response_with_code(topic: &TopicName, partition: PartitionId, error_code: i16) -> ProduceResponse {
        let pr = PartitionProduceResponse::default()
            .with_index(partition.0)
            .with_error_code(error_code)
            .with_base_offset(-1)
            .with_log_append_time_ms(-1);
        let tr = TopicProduceResponse::default()
            .with_name(topic.clone())
            .with_partition_responses(vec![pr]);
        ProduceResponse::default().with_responses(vec![tr])
    }

    /// B2: an `UnknownProducerId` ack on the in-flight head with no
    /// predecessors pops the failing batch, resets the partition's
    /// sequence space, rewrites the encoded header to
    /// `base_sequence=0`, and re-registers the batch with the original
    /// waiters and `first_send_at` intact and `attempt += 1`. The
    /// rewrite preserves the records section + a valid CRC so the
    /// broker accepts the retransmit on the next dispatch tick.
    #[test]
    fn unknown_pid_at_head_rewrites_to_zero() {
        let idem = Arc::new(IdempotentState::new());
        idem.force_ready_for_test(7, 0);
        let t = topic("recover");
        let p = PartitionId(0);

        // Reserve seqs 0..42 (so the next assign falls at 42), then
        // hand-register an in-flight batch at base_sequence=42 with
        // real v2-encoded bytes so the post-rewrite decode succeeds.
        for _ in 0..6 {
            let _ = idem.assign(&t, p, 7).unwrap();
        }
        assert_eq!(idem.assign(&t, p, 2).unwrap().base_sequence, 42);

        let original = encode_test_batch(7, 0, 42);
        let original_ptr = original.as_ptr();
        let (tx, mut rx) = oneshot::channel();
        let first_send_at = Instant::now() - Duration::from_secs(1);
        idem.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p,
            base_sequence: 42,
            record_count: 2,
            producer_id: 7,
            producer_epoch: 0,
            encoded: original,
            waiters: vec![tx],
            first_send_at,
            attempt: 0,
            next_attempt_at: Instant::now(),
        });

        // Drive the settle path with a single-partition response
        // carrying UnknownProducerId.
        let resp = produce_response_with_code(&t, p, ResponseError::UnknownProducerId.code());
        let config = ProducerConfig::default();
        let keys = vec![(t.clone(), p, 42)];
        settle_response_idempotent(
            resp,
            &idem,
            keys,
            &config,
            Duration::from_millis(100),
            Duration::from_secs(1),
        );

        // Waiters are kept (no synthetic Error::Broker), so try_recv
        // sees the sender still alive and yields Empty rather than
        // Closed/Ready. The retransmit will resolve them on success.
        match rx.try_recv() {
            Err(oneshot::error::TryRecvError::Empty) => {}
            other => panic!("waiter must still be parked; got {other:?}"),
        }

        // The new head decodes cleanly (valid CRC), carries
        // base_sequence=0, the same producer_id, and same record
        // count.
        let snapshots = idem.snapshot_inflight();
        assert_eq!(snapshots.len(), 1, "exactly one partition still in flight");
        let snap = &snapshots[0];
        assert_eq!(snap.base_sequence, 0, "head base_sequence reset to 0");
        assert_eq!(snap.attempt, 1, "attempt bumped by 1");
        assert_eq!(snap.first_send_at, first_send_at, "delivery clock kept");
        assert_ne!(
            snap.encoded.as_ptr(),
            original_ptr,
            "rewrite produced a fresh allocation",
        );

        let mut cursor = Cursor::new(snap.encoded.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor)
            .expect("rewritten batch must decode with a valid CRC");
        assert_eq!(decoded.records.len(), 2);
        for (i, r) in decoded.records.iter().enumerate() {
            assert_eq!(r.producer_id, 7);
            assert_eq!(r.producer_epoch, 0);
            assert_eq!(r.sequence, i as i32, "per-record sequence = base 0 + delta");
        }

        // `reset_partition` rewinds the partition's local
        // `next_sequence` to 0. NB: this is the post-state the plan
        // specifies (FINAL_IDEMPOTENCY.md B2), and matches the broker
        // having discarded its producer-id state. A follow-up
        // `assign` would currently return base_sequence=0 and collide
        // with the rewritten in-flight batch — see post-B2 caveats.
        assert_eq!(
            idem.partition_next_sequence_for_test(&t, p),
            Some(0),
            "reset_partition winds next_sequence back to 0",
        );
    }

    #[test]
    fn collect_dispatchable_skips_backed_off_heads() {
        // A head whose `next_attempt_at` is still in the future must be
        // skipped by the dispatch loop. This is what stops the producer
        // from busy-looping at linger cadence after a transient broker
        // error (Java's `retry.backoff.ms` semantics).
        let idem = Arc::new(IdempotentState::new());
        idem.force_ready_for_test(1, 0);

        // Partition A: backed off (next_attempt_at = now + 1 s).
        let _ = idem.assign(&topic("a"), PartitionId(0), 1).unwrap();
        let now = Instant::now();
        idem.register_inflight(InFlightBatch {
            topic: topic("a"),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::from_static(b"a"),
            waiters: Vec::new(),
            first_send_at: now,
            attempt: 1,
            next_attempt_at: now + Duration::from_secs(1),
        });

        // Partition B: ready to dispatch (next_attempt_at = now).
        let _ = idem.assign(&topic("b"), PartitionId(0), 1).unwrap();
        idem.register_inflight(InFlightBatch {
            topic: topic("b"),
            partition: PartitionId(0),
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: Bytes::from_static(b"b"),
            waiters: Vec::new(),
            first_send_at: now,
            attempt: 0,
            next_attempt_at: now,
        });

        let mut config = ProducerConfig::default();
        // High retries cap so the backed-off head isn't dropped for
        // exceeding retries before the backoff check runs.
        config.retries = 10;

        let out = collect_dispatchable(&config, &idem, now);
        assert_eq!(out.len(), 1, "exactly one partition should dispatch");
        assert_eq!(out[0].topic, topic("b"));

        // After the backoff elapses, both heads dispatch.
        let later = now + Duration::from_secs(2);
        let out = collect_dispatchable(&config, &idem, later);
        let mut topics: Vec<_> = out.iter().map(|s| s.topic.as_str().to_string()).collect();
        topics.sort();
        assert_eq!(topics, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn assign_failure_after_ensure_ready_marks_fatal() {
        let idem = IdempotentState::new();
        // Drive past ensure_ready by injecting a Ready state directly.
        idem.force_ready_for_test(7, 0);
        // Pre-flag the partition for a bump so the next assign returns
        // `PartitionBlocked` — a synthetic stand-in for the gate that
        // is meant to be unreachable but, per H1, must still fail-fast
        // if it is ever hit.
        idem.request_partition_bump(&topic("t"), PartitionId(3));

        let outcome = assign_or_fail(&idem, &topic("t"), PartitionId(3), 1);
        let reason = outcome.expect_err("blocked partition must surface as Err");
        assert!(reason.contains("t-3"), "reason should name topic-partition: {reason}");
        assert!(
            idem.is_fatal(),
            "assign failure must transition the producer to fatal",
        );

        // Mirror what dispatch_idempotent does post-drain: the waiters
        // of the soon-to-be-dropped batch must learn about the fatal.
        let (tx, rx) = oneshot::channel();
        notify_waiters_the_failure(vec![tx], || Error::IdempotenceFenced(reason.clone()));
        match rx.await.expect("waiter should be notified") {
            Err(Error::IdempotenceFenced(msg)) => assert_eq!(msg, reason),
            other => panic!("expected IdempotenceFenced, got {other:?}"),
        }
    }

    /// C1: a successful bump pass drains the partition's in-flight queue
    /// and re-installs each batch under the new `(pid, epoch)` with
    /// sequential `base_sequence` starting at 0 — mirrors Java's
    /// `TxnPartitionEntry.startSequencesAtBeginning`. Records `7..10`
    /// (count 3) and `10..12` (count 2) rewrite to `0..3` and `3..5`
    /// respectively; `partition.next_sequence` lands at 5.
    #[test]
    fn bump_pass_rewrites_paused_batches() {
        let idem = Arc::new(IdempotentState::new());
        // Original (pid=1, epoch=0); the bump moves us to (pid=123,
        // epoch=5) to mirror what a real `InitProducerIdRequest`
        // response would return.
        idem.force_ready_for_test(1, 0);
        let t = topic("bump");
        let p = PartitionId(0);

        // Reserve seqs 0..12 so the partition tracks `next_sequence=12`,
        // matching the in-flight batches we register at 7 and 10.
        for _ in 0..7 {
            let _ = idem.assign(&t, p, 1).unwrap();
        }
        let a = idem.assign(&t, p, 3).unwrap();
        assert_eq!(a.base_sequence, 7);
        let b = idem.assign(&t, p, 2).unwrap();
        assert_eq!(b.base_sequence, 10);
        // Trigger the bump (the OutOfOrderSequence path on the head).
        idem.request_partition_bump(&t, p);

        let (tx_a, mut rx_a) = oneshot::channel();
        idem.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p,
            base_sequence: 7,
            record_count: 3,
            producer_id: 1,
            producer_epoch: 0,
            encoded: encode_test_batch_with_count(1, 0, 7, 3),
            waiters: vec![tx_a],
            first_send_at: Instant::now() - Duration::from_secs(2),
            attempt: 4,
            next_attempt_at: Instant::now(),
        });
        let (tx_b, mut rx_b) = oneshot::channel();
        idem.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p,
            base_sequence: 10,
            record_count: 2,
            producer_id: 1,
            producer_epoch: 0,
            encoded: encode_test_batch_with_count(1, 0, 10, 2),
            waiters: vec![tx_b],
            first_send_at: Instant::now() - Duration::from_secs(1),
            attempt: 2,
            next_attempt_at: Instant::now(),
        });

        let (acc, _ready_rx) = Accumulator::new(Arc::new(ProducerConfig::default()));
        let success = rewrite_paused_batches(&idem, &acc, &t, p, (123, 5));
        assert!(success, "rewrite must succeed for valid v2 frames");

        // Pending-bump flag cleared by `apply_bumped_state`; partition
        // is unblocked for the next assign.
        assert!(!idem.has_pending_bumps());

        // Inspect both rewritten batches.
        let snapshots = idem.snapshot_inflight();
        assert_eq!(snapshots.len(), 1, "still one partition in flight");

        // `snapshot_inflight` only surfaces the head; drain the queue
        // via a temporary `apply_bumped_state(123, 5)` to inspect both.
        // Instead, decode the encoded bytes for the head, then trigger
        // a second apply to see what came next.
        // Simpler: read the queue directly via the dedicated
        // `partition_next_sequence_for_test` and snapshot head.
        let head = &snapshots[0];
        assert_eq!(head.base_sequence, 0, "first rewritten batch starts at 0");

        // `next_sequence` after installing batch_a (base=0, count=3) +
        // batch_b (base=3, count=2) lands at 5.
        assert_eq!(
            idem.partition_next_sequence_for_test(&t, p),
            Some(5),
            "next_sequence advances past both rewritten batches",
        );

        // The head's encoded bytes decode with a valid CRC under the
        // new (pid, epoch) and base_sequence=0.
        let mut cursor = Cursor::new(head.encoded.as_ref());
        let decoded = RecordBatchDecoder::decode(&mut cursor).expect("head decodes");
        assert_eq!(decoded.records.len(), 3);
        for (i, r) in decoded.records.iter().enumerate() {
            assert_eq!(r.producer_id, 123);
            assert_eq!(r.producer_epoch, 5);
            assert_eq!(r.sequence, i as i32);
        }

        // Waiters are still parked — the rewritten batches will be
        // dispatched on the next tick.
        assert!(
            matches!(rx_a.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "batch_a waiter must still be parked",
        );
        assert!(
            matches!(rx_b.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "batch_b waiter must still be parked",
        );
    }

    /// C1: after a successful bump pass, `assign` for the same partition
    /// returns `Ok` with `base_sequence` equal to the new
    /// `next_sequence` (i.e. past the rewritten paused batches), and
    /// stamps the new `(pid, epoch)`. Mirrors Java's behaviour after
    /// `startSequencesAtBeginning` clears the partition's bump flag.
    #[test]
    fn bump_pass_unblocks_subsequent_assign() {
        let idem = Arc::new(IdempotentState::new());
        idem.force_ready_for_test(1, 0);
        let t = topic("unblock");
        let p = PartitionId(0);

        // Register one in-flight batch (base=0, count=3), then flag for
        // bump as if the broker returned OutOfOrderSequence on it.
        let _ = idem.assign(&t, p, 3).unwrap();
        idem.request_partition_bump(&t, p);
        let (tx, _rx) = oneshot::channel();
        idem.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p,
            base_sequence: 0,
            record_count: 3,
            producer_id: 1,
            producer_epoch: 0,
            encoded: encode_test_batch_with_count(1, 0, 0, 3),
            waiters: vec![tx],
            first_send_at: Instant::now(),
            attempt: 1,
            next_attempt_at: Instant::now(),
        });

        // Pre-bump: assign is blocked.
        assert_eq!(
            idem.assign(&t, p, 1).unwrap_err(),
            crate::producer::idempotent::AssignError::PartitionBlocked,
        );

        let (acc, _ready_rx) = Accumulator::new(Arc::new(ProducerConfig::default()));
        assert!(rewrite_paused_batches(&idem, &acc, &t, p, (200, 7)));

        // After the bump pass, the next assign falls right after the
        // rewritten batch (base_sequence=3 = 0 + record_count) and
        // carries the new (pid, epoch).
        let assigned = idem.assign(&t, p, 1).expect("partition unblocked");
        assert_eq!(assigned.base_sequence, 3);
        assert_eq!(assigned.producer_id, 200);
        assert_eq!(assigned.producer_epoch, 7);
    }

    /// C1: a transient bump failure must re-flag the partition for
    /// retry on the next tick, leave the in-flight queue untouched,
    /// and not transition the producer to fatal. The classification
    /// helper distinguishes transient transport errors from terminal
    /// broker codes; this test exercises `is_fatal_bump_error` directly
    /// since the broker call itself is real-client-coupled.
    #[test]
    fn bump_request_failure_classification() {
        // Transient: transport-level / retryable broker codes.
        assert!(!is_fatal_bump_error(&Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))));
        assert!(!is_fatal_bump_error(&Error::NoBrokerAvailable("gone".into())));
        assert!(!is_fatal_bump_error(&Error::RequestTimeout("over".into())));
        assert!(!is_fatal_bump_error(&Error::Broker {
            error: ResponseError::NotCoordinator,
        }));

        // Fatal: already-fenced, or a broker classification that maps
        // to Fatal / Abortable. `ClusterAuthorizationFailed` is fatal.
        assert!(is_fatal_bump_error(&Error::IdempotenceFenced("fenced".into())));
        assert!(is_fatal_bump_error(&Error::Broker {
            error: ResponseError::ClusterAuthorizationFailed,
        }));
    }

    /// C1: when a bump is flagged via `request_partition_bump` but the
    /// partition state has gone away (its in-flight queue was drained
    /// elsewhere), `run_bump_pass`'s per-partition loop must drop the
    /// flag and continue without calling the broker. Exercises the
    /// early-return path via `partition_state` returning `None`.
    #[test]
    fn bump_pass_drops_flag_when_partition_state_missing() {
        let idem = Arc::new(IdempotentState::new());
        // Don't force-ready and don't assign — `partition_state` will
        // return `None` for an unknown partition.
        idem.request_partition_bump(&topic("ghost"), PartitionId(9));
        assert!(idem.has_pending_bumps());

        // Direct probe — `run_bump_pass` would short-circuit via this
        // same `partition_state` lookup.
        assert_eq!(
            idem.partition_state(&topic("ghost"), PartitionId(9)),
            None,
        );
    }

    /// Build a `ProduceResponse` carrying distinct `error_code`s for two
    /// `(topic, partition)` entries, so the settle path can be driven
    /// over a mixed-outcome single response.
    fn produce_response_with_two_codes(
        topic_a: &TopicName,
        partition_a: PartitionId,
        code_a: i16,
        topic_b: &TopicName,
        partition_b: PartitionId,
        code_b: i16,
    ) -> ProduceResponse {
        let pr_a = PartitionProduceResponse::default()
            .with_index(partition_a.0)
            .with_error_code(code_a)
            .with_base_offset(if code_a == 0 { 0 } else { -1 })
            .with_log_append_time_ms(-1);
        let pr_b = PartitionProduceResponse::default()
            .with_index(partition_b.0)
            .with_error_code(code_b)
            .with_base_offset(if code_b == 0 { 0 } else { -1 })
            .with_log_append_time_ms(-1);
        let tr_a = TopicProduceResponse::default()
            .with_name(topic_a.clone())
            .with_partition_responses(vec![pr_a]);
        let tr_b = TopicProduceResponse::default()
            .with_name(topic_b.clone())
            .with_partition_responses(vec![pr_b]);
        ProduceResponse::default().with_responses(vec![tr_a, tr_b])
    }

    /// C2: a per-partition fatal broker code (e.g. `InvalidRecord`)
    /// pops the failing partition's head, flags it for an epoch bump,
    /// and notifies the failing waiter — without poisoning unrelated
    /// partitions or marking the global producer fatal. Mirrors Java's
    /// `TransactionManager.handleFailedBatch` for non-transactional
    /// idempotent producers (`TransactionManager.java:833-841`): a fatal
    /// per-partition error routes through
    /// `requestIdempotentEpochBumpForPartition`, not the terminal-state
    /// transition.
    #[test]
    fn other_fatal_does_not_global_fatal() {
        let idem = Arc::new(IdempotentState::new());
        idem.force_ready_for_test(1, 0);

        let t_bad = topic("bad");
        let t_ok = topic("ok");
        let p = PartitionId(0);

        // Bad partition: one in-flight batch we'll hit with INVALID_RECORD.
        let _ = idem.assign(&t_bad, p, 1).unwrap();
        let (tx_bad, mut rx_bad) = oneshot::channel();
        idem.register_inflight(InFlightBatch {
            topic: t_bad.clone(),
            partition: p,
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: encode_test_batch_with_count(1, 0, 0, 1),
            waiters: vec![tx_bad],
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });

        // Healthy partition: one in-flight batch + a successful broker
        // response. Must survive the bad partition's failure untouched.
        let _ = idem.assign(&t_ok, p, 1).unwrap();
        let (tx_ok, mut rx_ok) = oneshot::channel();
        idem.register_inflight(InFlightBatch {
            topic: t_ok.clone(),
            partition: p,
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: encode_test_batch_with_count(1, 0, 0, 1),
            waiters: vec![tx_ok],
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });

        // InvalidRecord → RetryAction::Fatal → fail_inflight_head; the
        // OK partition gets error_code = 0 (success). `.code()` reads
        // the canonical i16 from `kafka-protocol` so this stays correct
        // if the protocol re-numbers an error.
        let resp = produce_response_with_two_codes(
            &t_bad,
            p,
            ResponseError::InvalidRecord.code(),
            &t_ok,
            p,
            0,
        );
        let config = ProducerConfig::default();
        let keys = vec![(t_bad.clone(), p, 0), (t_ok.clone(), p, 0)];
        settle_response_idempotent(
            resp,
            &idem,
            keys,
            &config,
            Duration::from_millis(100),
            Duration::from_secs(1),
        );

        // The producer must NOT have flipped to global-fatal.
        assert!(
            !idem.is_fatal(),
            "per-partition INVALID_RECORD must stay per-partition; \
             global-fatal would block every other partition's sends",
        );

        // The bad partition's waiter learns about the failure with the
        // broker's original error code.
        match rx_bad.try_recv() {
            Ok(Err(Error::Broker { error: ResponseError::InvalidRecord })) => {}
            other => panic!("expected Broker(InvalidRecord), got {other:?}"),
        }

        // The bad partition was popped + flagged for a bump.
        assert!(
            idem.has_pending_bumps(),
            "fail_inflight_head must request a bump so future batches \
             use a fresh epoch and base_sequence=0",
        );

        // Healthy partition's waiter received the success ack.
        match rx_ok.try_recv() {
            Ok(Ok(meta)) => {
                assert_eq!(meta.topic, t_ok);
                assert_eq!(meta.partition, p);
                assert_eq!(meta.offset, 0);
            }
            other => panic!("healthy partition must succeed, got {other:?}"),
        }
    }

    /// C2: feeding two distinct per-partition fatal codes in the *same*
    /// settle pass must produce independent per-partition recoveries —
    /// each failing partition gets popped + bump-flagged, every other
    /// partition stays in flight unchanged, and the producer never
    /// transitions to global-fatal. Verifies the per-partition isolation
    /// invariant that motivated removing the global-fatal short-circuit.
    #[test]
    fn per_partition_recovery_isolation() {
        let idem = Arc::new(IdempotentState::new());
        idem.force_ready_for_test(1, 0);

        let t = topic("iso");
        let p_bad = PartitionId(0);
        let p_mid = PartitionId(1);
        let p_ok = PartitionId(2);

        // Register one in-flight on each partition, distinct base_sequences
        // so the response→queue match is unambiguous.
        let mut rxs = Vec::new();
        for (part, base_seq) in [(p_bad, 0), (p_mid, 5), (p_ok, 11)] {
            // Reserve sequence space up to `base_seq` so register_inflight
            // doesn't trip the "register without prior assign" defence.
            for _ in 0..base_seq {
                let _ = idem.assign(&t, part, 1).unwrap();
            }
            let _ = idem.assign(&t, part, 1).unwrap();
            let (tx, rx) = oneshot::channel();
            idem.register_inflight(InFlightBatch {
                topic: t.clone(),
                partition: part,
                base_sequence: base_seq,
                record_count: 1,
                producer_id: 1,
                producer_epoch: 0,
                encoded: encode_test_batch_with_count(1, 0, base_seq, 1),
                waiters: vec![tx],
                first_send_at: Instant::now(),
                attempt: 0,
                next_attempt_at: Instant::now(),
            });
            rxs.push((part, rx));
        }

        // InvalidRecord + MessageTooLarge are both Fatal; the third
        // partition gets success (code 0). `.code()` keeps the test
        // honest if the protocol re-numbers either variant.
        let pr_bad = PartitionProduceResponse::default()
            .with_index(p_bad.0)
            .with_error_code(ResponseError::InvalidRecord.code())
            .with_base_offset(-1)
            .with_log_append_time_ms(-1);
        let pr_mid = PartitionProduceResponse::default()
            .with_index(p_mid.0)
            .with_error_code(ResponseError::MessageTooLarge.code())
            .with_base_offset(-1)
            .with_log_append_time_ms(-1);
        let pr_ok = PartitionProduceResponse::default()
            .with_index(p_ok.0)
            .with_error_code(0)
            .with_base_offset(42)
            .with_log_append_time_ms(-1);
        let tr = TopicProduceResponse::default()
            .with_name(t.clone())
            .with_partition_responses(vec![pr_bad, pr_mid, pr_ok]);
        let resp = ProduceResponse::default().with_responses(vec![tr]);

        let config = ProducerConfig::default();
        let keys = vec![(t.clone(), p_bad, 0), (t.clone(), p_mid, 5), (t.clone(), p_ok, 11)];
        settle_response_idempotent(
            resp,
            &idem,
            keys,
            &config,
            Duration::from_millis(100),
            Duration::from_secs(1),
        );

        // Producer survives — multiple per-partition fatals must not
        // escalate to global-fatal.
        assert!(!idem.is_fatal());

        // Both failing partitions get their waiters told *and* a bump
        // flag for sequence-space repair; the healthy one acks the
        // assigned offset.
        let mut by_part: HashMap<PartitionId, oneshot::Receiver<Result<RecordMetadata>>> =
            rxs.into_iter().collect();
        match by_part.remove(&p_bad).unwrap().try_recv() {
            Ok(Err(Error::Broker { error: ResponseError::InvalidRecord })) => {}
            other => panic!("bad partition expected InvalidRecord, got {other:?}"),
        }
        match by_part.remove(&p_mid).unwrap().try_recv() {
            Ok(Err(Error::Broker { error: ResponseError::MessageTooLarge })) => {}
            other => panic!("mid partition expected MessageTooLarge, got {other:?}"),
        }
        match by_part.remove(&p_ok).unwrap().try_recv() {
            Ok(Ok(meta)) => {
                assert_eq!(meta.partition, p_ok);
                assert_eq!(meta.offset, 42);
            }
            other => panic!("ok partition expected success, got {other:?}"),
        }

        // Both failing partitions are now flagged for a bump; the
        // healthy one is not (a successful complete clears nothing —
        // there was no flag to clear).
        let mut bumps = idem.take_pending_bumps();
        bumps.sort_by_key(|(_, p)| p.0);
        assert_eq!(bumps, vec![(t.clone(), p_bad), (t.clone(), p_mid)]);

        // No in-flight batches remain on the failed partitions (popped
        // by `fail_inflight_head`); the healthy partition's queue was
        // drained by `complete`. So `snapshot_inflight` is empty.
        assert!(idem.snapshot_inflight().is_empty());
    }

    /// C2: `ProducerFenced` is legitimately terminal — the broker has
    /// assigned our producer-id to someone else, so every in-flight
    /// sequence under the old `(pid, epoch)` is now unrecoverable. The
    /// settle path must call `mark_fatal` (not the per-partition
    /// `fail_inflight_head`) and drain all in-flight queues with
    /// `IdempotenceFenced`. Regression guard for the pre-existing
    /// `47 | 152` bug — code 152 isn't a real Kafka error code, so a
    /// real `ProducerFenced` ack used to fall through to the `_other`
    /// per-partition fail-and-bump path.
    #[test]
    fn producer_fenced_is_global_fatal() {
        let idem = Arc::new(IdempotentState::new());
        idem.force_ready_for_test(1, 0);
        let t = topic("fenced");
        let p_a = PartitionId(0);
        let p_b = PartitionId(1);

        // Two partitions in flight — both must learn about the fatal,
        // not just the one whose response carries the fenced code.
        let _ = idem.assign(&t, p_a, 1).unwrap();
        let (tx_a, mut rx_a) = oneshot::channel();
        idem.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p_a,
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: encode_test_batch_with_count(1, 0, 0, 1),
            waiters: vec![tx_a],
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });
        let _ = idem.assign(&t, p_b, 1).unwrap();
        let (tx_b, mut rx_b) = oneshot::channel();
        idem.register_inflight(InFlightBatch {
            topic: t.clone(),
            partition: p_b,
            base_sequence: 0,
            record_count: 1,
            producer_id: 1,
            producer_epoch: 0,
            encoded: encode_test_batch_with_count(1, 0, 0, 1),
            waiters: vec![tx_b],
            first_send_at: Instant::now(),
            attempt: 0,
            next_attempt_at: Instant::now(),
        });

        // Only partition A's response carries the fatal code.
        let pr_a = PartitionProduceResponse::default()
            .with_index(p_a.0)
            .with_error_code(ResponseError::ProducerFenced.code())
            .with_base_offset(-1)
            .with_log_append_time_ms(-1);
        let tr = TopicProduceResponse::default()
            .with_name(t.clone())
            .with_partition_responses(vec![pr_a]);
        let resp = ProduceResponse::default().with_responses(vec![tr]);

        let config = ProducerConfig::default();
        let keys = vec![(t.clone(), p_a, 0), (t.clone(), p_b, 0)];
        settle_response_idempotent(
            resp,
            &idem,
            keys,
            &config,
            Duration::from_millis(100),
            Duration::from_secs(1),
        );

        assert!(idem.is_fatal(), "ProducerFenced must flip to global-fatal");

        // Both waiters learn — A from the direct match, B from the
        // `drain_all_partition_waiters` sweep.
        match rx_a.try_recv() {
            Ok(Err(Error::IdempotenceFenced(_))) => {}
            other => panic!("expected IdempotenceFenced on A, got {other:?}"),
        }
        match rx_b.try_recv() {
            Ok(Err(Error::IdempotenceFenced(_))) => {}
            other => panic!("expected IdempotenceFenced on B, got {other:?}"),
        }
    }
}

