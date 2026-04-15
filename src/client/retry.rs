use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use kafka_protocol::messages::BrokerId;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;

use crate::broker::BrokerClient;
use crate::config::{Config, Security};
use crate::connection::Connection;
use crate::broker::Auth;
use crate::error::Result;

use super::ClientInner;

/// Accumulator to track which callers will be awakened for a given
/// broker id. On success, all senders are notified; on dial failure,
/// senders stay parked.
pub(super) type Inbox = Arc<Mutex<Vec<oneshot::Sender<BrokerClient>>>>;

/// Kills the wrapped task when this value is dropped.
pub(super) struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Describes the status per broker:
///
/// - `Resolved` — living `BrokerClient`. The happy path in
///   `Client::broker` finds it here and clones. NB: it may be shut
///   down; in that case the next `broker(id)` call evicts and respawns
///   a dialer.
/// - `Dialing` — a background task is already running to establish a
///   connection. A new caller pushes its own `oneshot::Sender` onto
///   `inbox` and awaits it.
pub(super) enum Slot {
    Resolved(BrokerClient),
    Dialing {
        inbox: Inbox,
        /// Drop the background task when this is dropped.
        abort_on_drop: AbortOnDrop,
    },
}

pub(super) type ConnectionMap = Mutex<HashMap<BrokerId, Slot>>;

/// Spawn a `perform_backoff_retry` task for `node_id` and return an
/// `AbortOnDrop` handle that cancels it when dropped.
///
/// NB: caller must hold the connections-map mutex.
/// Without it, this function opens a double-spawn race:
/// two concurrent callers each observe "no slot",
/// each spawn a dialer, and each try to install `Slot::Dialing` —
/// whichever insert lands second orphans the first dialer's
/// `AbortOnDrop` inside the clobbered slot, so nothing aborts the
/// orphan until `close()`.
pub(super) fn spawn_dialer(
    inner: &ClientInner,
    node_id: BrokerId,
    host: String,
    port: u16,
    inbox: Inbox,
    initial_times: u32,
) -> AbortOnDrop {
    // Weak reference allows to not have a long running task that holds all the connections
    // If closed, the following chain happens:
    // - Client drops → ClientInner drops
    // - ClientInner drops → the strong Arc<ConnectionMap> inside it drops
    // - Arc<ConnectionMap> drops → map drops
    // - map drops → every Slot::Dialing drops
    // - Slot::Dialing drops → AbortOnDrop fires
    // - AbortOnDrop fires → every dialer task is aborted at its next await
    let connections = Arc::downgrade(&inner.connections);
    let security = inner.security.clone();
    let auth = inner.auth.clone();
    let max_response_size = inner.max_response_size;
    let base = inner.reconnect_backoff;
    let max = inner.reconnect_backoff_max;
    let handle = tokio::spawn(perform_backoff_retry(
        inbox,
        connections,
        node_id,
        host,
        port,
        security,
        auth,
        max_response_size,
        base,
        max,
        initial_times,
    ));
    AbortOnDrop(handle.abort_handle())
}

/// Background dialer loop for a single broker id.
///
/// Owns the entire retry state machine: sleep, dial, success
/// notify. Runs until one of:
///
/// - **Success.** Notify every waiter a new the `BrokerClient` is connected,
///   overwrites the slot with `Slot::Resolved`, returns.
/// - **No waiters left.** After a sleep cycle, the inbox contains
///   only closed senders (every caller gave up) — remove the slot and
///   return. A subsequent `broker(id)` call respawns a fresh dialer.
/// - **Slot replaced.** `slot_is_ours` is false (metadata pruning +
///   respawn installed a fresh dialer for this id). Abandon silently.
/// - **Client gone.** `Weak::upgrade` fails — the entire `Client` has
///   been dropped.
/// - **Abort.** `close()` or metadata pruning drops the
///   `Slot::Dialing`, firing `AbortOnDrop` and cancelling this task at
///   its next await.
///
/// A failed dial simply logs, bumps `times`, and loops. The oneshot
/// senders in the inbox are untouched, so parked callers stay
/// `Pending` across any number of failed cycles. The only way a caller
/// observes failure is if they cancel their own future, or if the
/// dialer is aborted.
#[allow(clippy::too_many_arguments)]
async fn perform_backoff_retry(
    inbox: Inbox,
    connections: Weak<ConnectionMap>,
    node_id: BrokerId,
    host: String,
    port: u16,
    security: Security,
    auth: Auth,
    max_response_size: usize,
    base: Duration,
    max: Duration,
    initial_times: u32,
) {
    let mut times: u32 = initial_times;


    // Loop outcomes per iteration:
    // - success: under the map mutex, fan out the client to all
    //   waiters and replace the slot with `Slot::Resolved`.
    // - failure: log, bump `times`, loop back into the sleep.
    //   Waiters stay parked (Java semantics).
    // - no waiters: after a sleep cycle, if every sender in the
    //   inbox is closed (callers gave up), remove the slot and exit.
    loop {
        if times > 0 {
            tokio::time::sleep(next_backoff(times, base, max)).await;

            let Some(map_arc) = connections.upgrade() else {
                return;
            };
            let mut map = map_arc.lock().unwrap();

            // Metadata pruning + respawn may have replaced our slot
            // while we slept.
            if !slot_is_ours(&map, node_id, &inbox) {
                return;
            }
            {
                // NB: still keep map lock; it is important
                let mut guard = inbox.lock().unwrap();
                guard.retain(|tx| !tx.is_closed());
                if guard.is_empty() {
                    map.remove(&node_id);
                    return;
                }
            }
            // At least one live waiter remains: fall through and dial.
            drop(map);
        }

        let dial_result: Result<BrokerClient> = async {
            let config = Config::new(&host, port).with_max_response_size(max_response_size);
            let conn = Connection::connect(&config, security.clone()).await?;
            BrokerClient::new(conn, auth.clone()).await
        }
        .await;

        let client = match dial_result {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!(
                    node_id = node_id.0,
                    %host,
                    port,
                    error = %e,
                    "broker dial failed; will retry after backoff",
                );
                times = times.saturating_add(1);
                continue;
            }
        };

        let Some(map_arc) = connections.upgrade() else {
            return;
        };
        let mut map = map_arc.lock().unwrap();

        {
            let mut guard = inbox.lock().unwrap();
            for tx in guard.drain(..) {
                let _ = tx.send(client.clone());
            }
        }

        if !slot_is_ours(&map, node_id, &inbox) {
            return;
        }

        map.insert(node_id, Slot::Resolved(client));
        return;
    }
}

/// True iff the slot at `node_id` is `Dialing` with an inbox `Arc`
/// pointer-equal to ours. Used by `perform_backoff_retry` to detect
/// whether its slot has been replaced (metadata pruning + respawn) so
/// it can abandon the map entry rather than overwrite a newer dialer.
fn slot_is_ours(
    map: &std::sync::MutexGuard<'_, HashMap<BrokerId, Slot>>,
    node_id: BrokerId,
    inbox: &Inbox,
) -> bool {
    matches!(
        map.get(&node_id),
        Some(Slot::Dialing { inbox: slot_inbox, .. })
            if Arc::ptr_eq(slot_inbox, inbox)
    )
}

/// Reconnect backoff formula: `base * 2^(attempts-1)` capped at `max`,
/// with symmetric ±20 % jitter (Java `ExponentialBackoff`-style; simpler
/// than librdkafka's asymmetric −25 %/+50 % and serves the same
/// thundering-herd purpose). `attempts` is the number of consecutive
/// failures recorded, so `attempts == 1` returns ≈ `base`.
pub(crate) fn next_backoff(attempts: u32, base: Duration, max: Duration) -> Duration {
    let shift = attempts.saturating_sub(1).min(31);
    // Multiply in nanos to keep the math in one unit. Both saturating_mul
    // and the cap keep us safely inside u128.
    let multiplier: u128 = 1u128 << shift;
    let nominal = base
        .as_nanos()
        .saturating_mul(multiplier)
        .min(max.as_nanos());
    // ±20 % jitter. Using `fastrand` (already a dependency) — reseeded
    // per-call via the thread-local RNG.
    let jitter_span = (nominal / 5) as i128; // 20 % of nominal
    let jitter = if jitter_span > 0 {
        fastrand::i128(-jitter_span..=jitter_span)
    } else {
        0
    };
    let result = (nominal as i128 + jitter).max(0) as u128;
    // Cap at u64::MAX nanos — Duration::from_nanos expects u64.
    let nanos_u64 = result.min(u64::MAX as u128) as u64;
    Duration::from_nanos(nanos_u64)
}
