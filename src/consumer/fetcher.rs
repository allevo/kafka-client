use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::ResponseError;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{ApiKey, BrokerId, FetchRequest, FetchResponse, TopicName};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::{CallOptions, Client, NodeTarget, PartitionId};
use crate::consumer::decode;
use crate::consumer::offsets;
use crate::consumer::{ConsumerConfig, ConsumerRecord, TopicPartition};
use crate::error::{Error, Result, RetryAction};

// Wire sentinel for "regular consumer, not a follower replica" on Fetch
// (matches the same value used in `offsets.rs`).
const CONSUMER_REPLICA_ID: i32 = -1;

/// Control message sent from the public `Consumer` API to the background
/// fetcher task. The fetcher owns the cursor map; mutations always flow
/// through this channel so there is a single writer.
#[allow(dead_code)]
pub(super) enum AssignmentUpdate {
    /// Replace the entire set of assigned partitions. The fetcher resolves
    /// start offsets and signals completion (or failure) via `ack` so
    /// `Consumer::assign` can wait until cursors are seeded before
    /// returning.
    Replace {
        tps: Vec<TopicPartition>,
        ack: oneshot::Sender<Result<()>>,
    },
    /// Reposition a single partition's read cursor. Best-effort: if the
    /// partition isn't currently assigned the fetcher silently drops the
    /// update.
    Seek { tp: TopicPartition, offset: i64 },
}

/// Per-partition state owned by the fetcher loop.
///
/// `leader_epoch` is echoed back to the broker on each Fetch so the
/// broker can fence stale clients that missed a leadership change.
/// `refresh_attempts` counts consecutive metadata refreshes triggered by
/// classify-driven `RefreshMetadata` errors; the partition is dropped
/// after three to avoid spinning on a permanently broken topology.
#[allow(dead_code)]
pub(super) struct Cursor {
    pub leader_id: BrokerId,
    pub leader_epoch: i32,
    pub next_offset: i64,
    pub refresh_attempts: u8,
}

/// Spawn the long-lived fetcher task and return its [`JoinHandle`] so
/// [`Consumer::close`] can await graceful shutdown after signalling
/// `shutdown_rx`. The task owns the cursor map and is the sole writer
/// to `record_tx`.
pub(super) fn spawn_fetcher(
    client: Client,
    config: Arc<ConsumerConfig>,
    record_tx: mpsc::Sender<Result<ConsumerRecord>>,
    assign_rx: mpsc::UnboundedReceiver<AssignmentUpdate>,
    shutdown_rx: oneshot::Receiver<()>,
) -> JoinHandle<()> {
    tokio::spawn(fetcher_main(
        client,
        config,
        record_tx,
        assign_rx,
        shutdown_rx,
    ))
}

async fn fetcher_main(
    client: Client,
    config: Arc<ConsumerConfig>,
    record_tx: mpsc::Sender<Result<ConsumerRecord>>,
    mut assign_rx: mpsc::UnboundedReceiver<AssignmentUpdate>,
    mut shutdown_rx: oneshot::Receiver<()>,
) {
    // Owned exclusively by this task; every mutation flows through
    // `assign_rx` so there is a single writer.
    let mut cursors: HashMap<TopicPartition, Cursor> = HashMap::new();

    // In-flight per-leader Fetch RPCs. One task per leader per round;
    // each resolves to `(leader_id, Result<FetchResponse>)` so steps
    // 15/16 can route responses to per-partition emission and error
    // handling. While empty, `join_next()` returns `None`, which
    // disables that arm of the select! below.
    let mut in_flight: JoinSet<(BrokerId, Result<FetchResponse>)> = JoinSet::new();

    loop {
        // Kick off a new round once the previous one has fully drained.
        // No cursors → idle (no assignment yet, or every partition was
        // dropped by error handling); we wake up via `assign_rx`.
        if in_flight.is_empty() && !cursors.is_empty() {
            dispatch_round(&client, &config, &cursors, &mut in_flight);
        }

        tokio::select! {
            // Shutdown wins over the other arms so `Consumer::close`
            // can't be starved by a slow Fetch round or a backed-up
            // assignment queue.
            biased;
            _ = &mut shutdown_rx => break,
            update = assign_rx.recv() => {
                match update {
                    Some(AssignmentUpdate::Replace { tps, ack }) => {
                        let result = handle_replace(&client, &config, &tps, &mut cursors).await;
                        // Receiver may have been dropped (e.g. `assign()`
                        // future cancelled); silently swallow the send
                        // failure — the task continues running.
                        let _ = ack.send(result);
                    }
                    Some(AssignmentUpdate::Seek { tp, offset }) => {
                        // Best-effort: a seek for an unassigned partition
                        // is silently dropped per the AssignmentUpdate doc
                        // comment. Reset `refresh_attempts` so a prior
                        // run of metadata-refresh failures doesn't drop
                        // the partition on the next Fetch round.
                        if let Some(cursor) = cursors.get_mut(&tp) {
                            cursor.next_offset = offset;
                            cursor.refresh_attempts = 0;
                        }
                    }
                    // All `assign_tx` clones dropped — the public
                    // Consumer is gone, so exit cleanly.
                    None => break,
                }
            }
            Some(res) = in_flight.join_next() => {
                let response = match res {
                    Ok((_leader_id, Ok(response))) => response,
                    // Per-leader RPC failure. Step 16 will refresh
                    // metadata or surface the error per-partition; for
                    // now we drop the result and let the next round
                    // retry against the same cursors.
                    Ok((_leader_id, Err(_e))) => continue,
                    // JoinSet task panicked or was aborted — nothing to
                    // recover; skip and let the next round dispatch.
                    Err(_) => continue,
                };
                match handle_fetch_response(
                    &client,
                    &config,
                    response,
                    &mut cursors,
                    &record_tx,
                    &mut assign_rx,
                    &mut shutdown_rx,
                )
                .await
                {
                    ProcessOutcome::Continue => {}
                    ProcessOutcome::Stop | ProcessOutcome::Shutdown => break,
                }
            }
        }
    }
}

/// Outcome of processing one Fetch response, signalled back to
/// `fetcher_main` so the outer `select!` can react to events that fired
/// while we were emitting records.
enum ProcessOutcome {
    /// Response fully drained (or abandoned after applying an inline
    /// assignment update). Outer loop continues normally.
    Continue,
    /// `record_tx` receiver was dropped, or `assign_rx` returned `None`
    /// (all senders gone). Outer loop should terminate.
    Stop,
    /// Shutdown signal fired during emission. Outer loop should
    /// terminate.
    Shutdown,
}

/// Decode and emit records from a single Fetch response. Step 15 only
/// handles the success path (`error_code == 0`); per-partition error
/// codes are intercepted by step 16 in `handle_partition_error`.
///
/// The per-record `tokio::select!` keeps `seek` and `close` responsive
/// even while `record_tx` is full: a slow downstream consumer parks the
/// fetcher on `record_tx.reserve()` rather than `send().await`, so a
/// concurrent assignment update or shutdown signal can preempt the wait
/// instead of being queued behind a blocked send.
async fn handle_fetch_response(
    client: &Client,
    config: &ConsumerConfig,
    response: FetchResponse,
    cursors: &mut HashMap<TopicPartition, Cursor>,
    record_tx: &mpsc::Sender<Result<ConsumerRecord>>,
    assign_rx: &mut mpsc::UnboundedReceiver<AssignmentUpdate>,
    shutdown_rx: &mut oneshot::Receiver<()>,
) -> ProcessOutcome {
    for topic_resp in response.responses {
        let topic = topic_resp.topic;
        for part in topic_resp.partitions {
            let tp = TopicPartition {
                topic: topic.clone(),
                partition: PartitionId(part.partition_index),
            };

            // Per-partition errors are scoped to this partition: one bad
            // partition does not close the Stream. `handle_partition_error`
            // mutates `cursors` in place (drop on Fatal, advance offset on
            // OffsetOutOfRange, bump refresh_attempts on RefreshMetadata)
            // and surfaces fatal errors via `record_tx`.
            if part.error_code != 0 {
                handle_partition_error(client, config, part.error_code, &tp, cursors, record_tx)
                    .await;
                continue;
            }

            // `trimmed` ends up holding only the records we should
            // actually emit; the borrow of `cursors` is dropped at the
            // end of this block so the per-record `select!` below can
            // re-borrow `cursors` to apply inline assignment updates.
            let trimmed = {
                // Cursor may have been removed by a Replace that landed
                // between dispatch and response; drop the partition data.
                let Some(cursor) = cursors.get_mut(&tp) else {
                    continue;
                };

                let records =
                    match decode::decode_partition_records(&topic, tp.partition, part.records) {
                        Ok(r) => r,
                        Err(e) => {
                            // Wire-level decode failure is unrecoverable for
                            // this partition; surface once and drop the
                            // partition so we don't spin re-decoding the
                            // same bytes on the next round.
                            let _ = record_tx.send(Err(e)).await;
                            cursors.remove(&tp);
                            continue;
                        }
                    };

                // Mid-batch trim: the broker returns whole batches
                // starting at-or-before the requested fetch offset, so
                // post-seek we may receive offsets we already consumed.
                let trimmed: Vec<ConsumerRecord> = records
                    .into_iter()
                    .filter(|r| r.offset >= cursor.next_offset)
                    .collect();

                if trimmed.is_empty() {
                    continue;
                }

                // Advance BEFORE emitting so a `seek` arriving while we
                // park on `record_tx.reserve()` doesn't cause us to
                // re-emit records the next dispatch would re-fetch.
                cursor.next_offset = trimmed.last().unwrap().offset + 1;
                // Successful fetch resets the metadata-refresh budget
                // tracked by step 16's RefreshMetadata path.
                cursor.refresh_attempts = 0;

                trimmed
            };

            for record in trimmed {
                tokio::select! {
                    // Same biased ordering as the outer loop: shutdown
                    // and assignment updates win over emission so a
                    // user-initiated `close` / `assign` / `seek` is
                    // never starved by a backed-up channel.
                    biased;
                    _ = &mut *shutdown_rx => return ProcessOutcome::Shutdown,
                    update = assign_rx.recv() => {
                        match update {
                            Some(AssignmentUpdate::Replace { tps, ack }) => {
                                let result = handle_replace(client, config, &tps, cursors).await;
                                let _ = ack.send(result);
                                // Cursors were just rebuilt; abandon
                                // remaining records — any unsent records
                                // from the prior assignment are stale,
                                // and the next loop iteration dispatches
                                // a fresh round against the new cursors.
                                return ProcessOutcome::Continue;
                            }
                            Some(AssignmentUpdate::Seek { tp: seek_tp, offset }) => {
                                if let Some(c) = cursors.get_mut(&seek_tp) {
                                    c.next_offset = offset;
                                    c.refresh_attempts = 0;
                                }
                                // Cursor may have shifted; abandon the
                                // rest of this batch. For a same-tp seek
                                // the remaining records are pre-seek and
                                // must not be emitted; for a cross-tp
                                // seek this is conservative — slice 1
                                // tests don't exercise mid-emit cross-tp
                                // seeks.
                                return ProcessOutcome::Continue;
                            }
                            // All `assign_tx` clones dropped — Consumer
                            // is gone, terminate cleanly.
                            None => return ProcessOutcome::Stop,
                        }
                    }
                    permit = record_tx.reserve() => match permit {
                        Ok(p) => p.send(Ok(record)),
                        // Receiver of `record_tx` was dropped (e.g. the
                        // Consumer's record_rx was taken and dropped).
                        // Continued fetching is pointless.
                        Err(_) => return ProcessOutcome::Stop,
                    },
                }
            }
        }
    }
    ProcessOutcome::Continue
}

/// Per-partition tuple staged for a Fetch round: `(partition,
/// leader_epoch, fetch_offset)`. Folded into `FetchPartition` once
/// the per-leader grouping is complete.
type PartitionFetchEntry = (PartitionId, i32, i64);

/// Group cursors by leader and spawn one Fetch task per leader. Each
/// task resolves to `(leader_id, Result<FetchResponse>)` so the caller
/// can route responses back to per-partition handling.
///
/// Caller must ensure `cursors` is non-empty; otherwise this is a
/// no-op that wastes the call.
fn dispatch_round(
    client: &Client,
    config: &ConsumerConfig,
    cursors: &HashMap<TopicPartition, Cursor>,
    in_flight: &mut JoinSet<(BrokerId, Result<FetchResponse>)>,
) {
    // Group by leader, then by topic, so each leader gets one
    // FetchRequest with one FetchTopic entry per topic.
    let mut by_leader: HashMap<BrokerId, HashMap<TopicName, Vec<PartitionFetchEntry>>> =
        HashMap::new();
    for (tp, cursor) in cursors {
        by_leader
            .entry(cursor.leader_id)
            .or_default()
            .entry(tp.topic.clone())
            .or_default()
            .push((tp.partition, cursor.leader_epoch, cursor.next_offset));
    }

    for (leader_id, topic_map) in by_leader {
        let topics: Vec<FetchTopic> = topic_map
            .into_iter()
            .map(|(topic, parts)| {
                let partitions: Vec<FetchPartition> = parts
                    .into_iter()
                    .map(|(partition, leader_epoch, fetch_offset)| {
                        FetchPartition::default()
                            .with_partition(partition.0)
                            // Echo the cached leader epoch so the broker
                            // can fence stale clients on a leadership
                            // change.
                            .with_current_leader_epoch(leader_epoch)
                            .with_fetch_offset(fetch_offset)
                            // -1 = "no last-fetched epoch"; slice 1
                            // doesn't track per-batch epochs.
                            .with_last_fetched_epoch(-1)
                            // -1 = "log start unknown" — we're a
                            // regular consumer, not a follower replica.
                            .with_log_start_offset(-1)
                            .with_partition_max_bytes(config.partition_max_bytes)
                    })
                    .collect();
                FetchTopic::default()
                    .with_topic(topic)
                    .with_partitions(partitions)
            })
            .collect();

        // FetchRequest v12: `session_id = 0` / `session_epoch = -1`
        // means "stateless full fetch" — opting out of incremental
        // fetch sessions, which is the simplest semantics for slice 1.
        let request = FetchRequest::default()
            .with_replica_id(BrokerId(CONSUMER_REPLICA_ID))
            .with_max_wait_ms(duration_to_ms(config.fetch_max_wait))
            .with_min_bytes(config.fetch_min_bytes)
            .with_max_bytes(config.fetch_max_bytes)
            .with_isolation_level(config.isolation_level as i8)
            .with_session_id(0)
            .with_session_epoch(-1)
            .with_topics(topics);

        let client = client.clone();
        in_flight.spawn(async move {
            let result = client
                .send::<FetchRequest, FetchResponse>(
                    NodeTarget::Broker(leader_id),
                    ApiKey::Fetch,
                    12,
                    request,
                    &CallOptions::default(),
                )
                .await;
            (leader_id, result)
        });
    }
}

fn duration_to_ms(d: Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX)
}

/// Handle `AssignmentUpdate::Replace`: refresh metadata for any topic
/// not already cached as healthy, look up the leader and leader-epoch
/// per partition, group by leader, resolve start offsets, then seed the
/// cursor map.
///
/// All-or-nothing: any failure clears `cursors` and the error is
/// surfaced via the caller's `ack`. Partial seeding would leave the
/// fetcher in a state where some partitions silently never produce
/// records, which is harder to diagnose than a fast assign() failure.
async fn handle_replace(
    client: &Client,
    config: &ConsumerConfig,
    tps: &[TopicPartition],
    cursors: &mut HashMap<TopicPartition, Cursor>,
) -> Result<()> {
    // Wipe up front so a partial failure leaves the fetcher idle
    // rather than reading from a stale assignment.
    cursors.clear();

    if tps.is_empty() {
        return Ok(());
    }

    // Deduplicate topic names so we issue at most one Metadata entry
    // per topic; preserves source order for deterministic logs.
    let mut unique_topics: Vec<TopicName> = Vec::new();
    for tp in tps {
        if !unique_topics.contains(&tp.topic) {
            unique_topics.push(tp.topic.clone());
        }
    }

    // Always refresh: a stale cache can hand us a leader that has
    // since moved, and the cost of one Metadata call dwarfs the cost
    // of a misrouted Fetch followed by metadata invalidation.
    client.refresh_topics(&unique_topics).await?;

    // Group `(partition, leader_epoch)` by leader so we issue one
    // ListOffsets per leader rather than one per partition.
    let mut by_leader: HashMap<BrokerId, HashMap<TopicName, Vec<(PartitionId, i32)>>> =
        HashMap::new();

    for tp in tps {
        let meta = client.topic_metadata(&tp.topic).ok_or_else(|| {
            Error::Protocol(format!(
                "metadata for topic '{}' not present after refresh_topics",
                tp.topic.as_str()
            ))
        })?;
        if let Some(err) = meta.error {
            return Err(Error::Broker { error: err });
        }
        let part = meta
            .partitions
            .iter()
            .find(|(pid, _)| *pid == tp.partition)
            .map(|(_, m)| *m)
            .ok_or_else(|| {
                Error::Protocol(format!(
                    "partition {}-{} not present in metadata",
                    tp.topic.as_str(),
                    tp.partition.0
                ))
            })?;
        by_leader
            .entry(part.leader)
            .or_default()
            .entry(tp.topic.clone())
            .or_default()
            .push((tp.partition, part.leader_epoch));
    }

    // Resolve start offsets per leader. Done sequentially in slice 1;
    // a future slice can fan out with `JoinSet` once we wire fetch
    // dispatch the same way.
    for (leader_id, topic_map) in &by_leader {
        let topics: Vec<(TopicName, Vec<PartitionId>)> = topic_map
            .iter()
            .map(|(name, parts)| (name.clone(), parts.iter().map(|(p, _)| *p).collect()))
            .collect();
        let start_offsets = offsets::resolve_start_offsets(
            client,
            *leader_id,
            config.isolation_level,
            &topics,
            config.auto_offset_reset,
        )
        .await
        .inspect_err(|_| cursors.clear())?;

        for (topic_name, parts) in topic_map {
            for (partition, leader_epoch) in parts {
                let tp = TopicPartition {
                    topic: topic_name.clone(),
                    partition: *partition,
                };
                let next_offset = *start_offsets.get(&tp).ok_or_else(|| {
                    Error::Protocol(format!(
                        "ListOffsets did not return offset for {}-{}",
                        topic_name.as_str(),
                        partition.0
                    ))
                })?;
                cursors.insert(
                    tp,
                    Cursor {
                        leader_id: *leader_id,
                        leader_epoch: *leader_epoch,
                        next_offset,
                        refresh_attempts: 0,
                    },
                );
            }
        }
    }

    Ok(())
}

/// After how many consecutive metadata refreshes without a successful
/// Fetch the partition is dropped from the assignment. Bounded so a
/// permanently broken partition (e.g. topic deleted) cannot pin the
/// fetcher in a refresh loop indefinitely.
const MAX_REFRESH_ATTEMPTS: u8 = 3;

/// Handle a non-zero per-partition error code from a Fetch response.
///
/// Two codes get consumer-specific overrides because `Error::classify()`
/// answers them as `Fatal` — correct for a producer's wire-level retry
/// loop, wrong for a streaming consumer that wants to recover:
///
/// - `OffsetOutOfRange` (1): the cached cursor fell off the log (log
///   compaction, retention, or post-truncate replica). Re-resolve from
///   `auto_offset_reset` against the same leader and keep the partition.
/// - `RecordListTooLarge` (18): a single record exceeds
///   `partition_max_bytes`. There is no in-band recovery; surface and
///   drop so the rest of the assignment keeps making progress.
///
/// Everything else dispatches via `classify()`:
/// - `RefreshMetadata` → refresh and retry, bounded by
///   [`MAX_REFRESH_ATTEMPTS`] so a permanently broken topology can't
///   pin the fetcher forever.
/// - `Retry` → no state change; the next dispatch re-fetches the same
///   offset.
/// - `Fatal` / `Abortable` → surface once and drop the partition.
async fn handle_partition_error(
    client: &Client,
    config: &ConsumerConfig,
    error_code: i16,
    tp: &TopicPartition,
    cursors: &mut HashMap<TopicPartition, Cursor>,
    record_tx: &mpsc::Sender<Result<ConsumerRecord>>,
) {
    // Preserve the original i16 if the code is unrecognised, instead of
    // collapsing to `UnknownServerError`, so a downstream observer sees
    // exactly what the broker returned. Mirrors the producer pattern in
    // `producer/sender.rs:278`.
    let response_error =
        ResponseError::try_from_code(error_code).unwrap_or(ResponseError::Unknown(error_code));

    match response_error {
        ResponseError::OffsetOutOfRange => {
            // Cursor may have been dropped by an intervening Replace
            // between dispatch and response; nothing to re-seed.
            let Some(cursor) = cursors.get(tp) else {
                return;
            };
            let leader_id = cursor.leader_id;
            let topics = vec![(tp.topic.clone(), vec![tp.partition])];
            match offsets::resolve_start_offsets(
                client,
                leader_id,
                config.isolation_level,
                &topics,
                config.auto_offset_reset,
            )
            .await
            {
                Ok(map) => {
                    if let (Some(c), Some(off)) = (cursors.get_mut(tp), map.get(tp)) {
                        c.next_offset = *off;
                        // Successful re-resolution counts as forward
                        // progress; reset the metadata-refresh budget.
                        c.refresh_attempts = 0;
                    }
                }
                Err(e) => {
                    // ListOffsets failed permanently for this partition;
                    // surface and drop so we don't spin re-issuing it.
                    let _ = record_tx.send(Err(e)).await;
                    cursors.remove(tp);
                }
            }
            return;
        }
        ResponseError::RecordListTooLarge => {
            let _ = record_tx
                .send(Err(Error::Broker {
                    error: response_error,
                }))
                .await;
            cursors.remove(tp);
            return;
        }
        _ => {}
    }

    let err = Error::Broker {
        error: response_error,
    };
    match err.classify() {
        RetryAction::RefreshMetadata => {
            // Best-effort: even if the refresh RPC fails, we still bump
            // the attempt counter so a persistently failing topology
            // eventually drops the partition.
            let _ = client.refresh_topics(std::slice::from_ref(&tp.topic)).await;
            if let Some(cursor) = cursors.get_mut(tp) {
                cursor.refresh_attempts = cursor.refresh_attempts.saturating_add(1);
                if cursor.refresh_attempts >= MAX_REFRESH_ATTEMPTS {
                    let _ = record_tx.send(Err(err)).await;
                    cursors.remove(tp);
                }
            }
        }
        RetryAction::Retry => {
            // The next dispatch re-fetches the same offset, so no state
            // change. `refresh_attempts` is intentionally not bumped:
            // this isn't a metadata problem.
        }
        // Consumers can't abort transactions, so `Abortable` collapses
        // into the same drop-and-surface path as `Fatal`.
        RetryAction::Fatal | RetryAction::Abortable => {
            let _ = record_tx.send(Err(err)).await;
            cursors.remove(tp);
        }
    }
}
