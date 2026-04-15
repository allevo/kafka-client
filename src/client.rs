use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use arc_swap::ArcSwap;
use kafka_protocol::messages::{BrokerId, MetadataResponse};
use tokio::sync::oneshot;
use tokio::task::AbortHandle;

use crate::admin::AdminClient;
use crate::broker::{Auth, BrokerClient};
use crate::config::{Config, Security};
use crate::connection::Connection;
use crate::error::{Error, Result};

/// Translates broker addresses from metadata into actual connectable addresses.
///
/// Receives `(node_id, advertised_host, advertised_port)` and returns
/// `(actual_host, actual_port)`. Useful when brokers sit behind NAT,
/// Docker port mapping, or a proxy.
type AddressResolver = Arc<dyn Fn(BrokerId, &str, i32) -> Result<(String, u16)> + Send + Sync>;

struct BrokerInfo {
    host: String,
    port: u16,
}

/// Immutable view of the cluster. This is the cached metadata response.
struct MetadataSnapshot {
    controller_id: BrokerId,
    brokers: HashMap<BrokerId, BrokerInfo>,
}

/// Accumulator to track which callers will be awakened for a given
/// broker id. On success, all senders are notified; on dial failure,
/// senders stay parked.
type Inbox = Arc<Mutex<Vec<oneshot::Sender<BrokerClient>>>>;

/// Kills the wrapped task when this value is dropped.
struct AbortOnDrop(AbortHandle);

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
enum Slot {
    Resolved(BrokerClient),
    Dialing {
        inbox: Inbox,
        /// Drop the background task when this is dropped.
        abort_on_drop: AbortOnDrop,
    },
}

type ConnectionMap = Mutex<HashMap<BrokerId, Slot>>;

/// Shared state behind every `Client` clone. Held inside an `Arc` so the
/// public `Client` handle is cheap to clone and pass between tokio tasks.
struct ClientInner {
    security: Security,
    auth: Auth,
    /// Captured from the bootstrap config that succeeded; reused when opening connections
    /// to brokers discovered via metadata.
    max_response_size: usize,
    address_resolver: Option<AddressResolver>,

    reconnect_backoff: Duration,
    reconnect_backoff_max: Duration,

    metadata: ArcSwap<MetadataSnapshot>,

    connections: Arc<ConnectionMap>,
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub async fn connect(bootstrap: &[Config], security: Security, auth: Auth) -> Result<Self> {
        Self::connect_inner(bootstrap, security, auth, None).await
    }

    pub async fn connect_with_resolver(
        bootstrap: &[Config],
        security: Security,
        auth: Auth,
        resolver: impl Fn(BrokerId, &str, i32) -> Result<(String, u16)> + Send + Sync + 'static,
    ) -> Result<Self> {
        Self::connect_inner(bootstrap, security, auth, Some(Arc::new(resolver))).await
    }

    async fn connect_inner(
        bootstrap: &[Config],
        security: Security,
        auth: Auth,
        address_resolver: Option<AddressResolver>,
    ) -> Result<Self> {
        let mut last_err = None;

        for config in bootstrap {
            match Connection::connect(config, security.clone()).await {
                Ok(conn) => {
                    let broker = BrokerClient::new(conn, auth.clone()).await?;
                    let metadata = broker.fetch_metadata().await?;

                    let mut brokers = HashMap::with_capacity(metadata.brokers.len());
                    let mut bootstrap_node_id = None;

                    for broker in &metadata.brokers {
                        let (host, port) = resolve_address(
                            &address_resolver,
                            broker.node_id,
                            broker.host.as_str(),
                            broker.port,
                        )?;
                        if bootstrap_node_id.is_none() && host == config.host && port == config.port
                        {
                            bootstrap_node_id = Some(broker.node_id);
                        }
                        brokers.insert(broker.node_id, BrokerInfo { host, port });
                    }

                    // Seed the connection map with the bootstrap broker only when we
                    // can match its wire address to a metadata entry — otherwise we'd
                    // be inserting a connection-to-host-X under broker-id-Y, which
                    // gives later callers the wrong broker. If unmatched, we drop the
                    // bootstrap connection and let the next `broker(id)` call dial fresh.
                    let mut connections: HashMap<BrokerId, Slot> = HashMap::new();
                    if let Some(node_id) = bootstrap_node_id {
                        connections.insert(node_id, Slot::Resolved(broker));
                    }

                    let inner = ClientInner {
                        security,
                        auth,
                        max_response_size: config.max_response_size,
                        address_resolver,
                        reconnect_backoff: config.reconnect_backoff,
                        reconnect_backoff_max: config.reconnect_backoff_max,
                        metadata: ArcSwap::new(Arc::new(MetadataSnapshot {
                            controller_id: metadata.controller_id,
                            brokers,
                        })),
                        connections: Arc::new(Mutex::new(connections)),
                    };

                    return Ok(Client {
                        inner: Arc::new(inner),
                    });
                }
                Err(e) => {
                    tracing::warn!(host = %config.host, port = config.port, error = %e, "bootstrap broker unreachable");

                    // We use the first "good" broker, ignoring the before errors.
                    last_err = Some(e);
                }
            }
        }

        Err(last_err
            .unwrap_or_else(|| Error::ProtocolError("no bootstrap brokers provided".into())))
    }

    pub async fn controller(&self) -> Result<BrokerClient> {
        let id = self.inner.metadata.load().controller_id;
        self.broker(id).await
    }

    /// Return a usable `BrokerClient` for `node_id`.
    ///
    /// Fast path: under the connections-map mutex, return a clone of the
    /// cached `Slot::Resolved` client if it's live and the id is still
    /// known to metadata.
    ///
    /// Cold path: spawn (or join) a per-broker `perform_backoff_retry`
    /// task. Each caller pushes a fresh `oneshot::Sender` into the
    /// dialer's `Inbox` and awaits the result.
    pub async fn broker(&self, node_id: BrokerId) -> Result<BrokerClient> {
        // Validate membership before touching the slot map so a bogus id
        // doesn't leak an empty slot.
        let (host, port) = {
            let snap = self.inner.metadata.load();
            let info = snap.brokers.get(&node_id).ok_or_else(|| {
                Error::ProtocolError(format!("unknown broker node_id: {}", node_id.0))
            })?;
            (info.host.clone(), info.port)
        };

        let rx = {
            let mut map = self.inner.connections
                .lock()
                .unwrap();

            // Fast path: live cached client.
            if let Some(Slot::Resolved(c)) = map.get(&node_id) {
                if !c.is_shutdown() {
                    return Ok(c.clone());
                }
            }

            // Cold path:
            // - absent slot: spawn dialer with the oneshot sender.
            // - an existing `Dialing`: add the oneshot sender to the list.
            // - an existing `Resolved`: it is shut down, so we evict and
            //   respawn a dialer — almost like the absent-slot case.
            // Both spawn branches call `spawn_dialer` while still holding
            // the map mutex — see the `spawn_dialer` doc for the race
            // this closes.
            let (tx, rx) = oneshot::channel();
            match map.entry(node_id) {
                Entry::Occupied(mut e) => match e.get_mut() {
                    Slot::Dialing { inbox, .. } => {
                        inbox.lock().unwrap().push(tx);
                    }
                    Slot::Resolved(s) => {
                        assert!(s.is_shutdown(), "BrokerClient is shut down");

                        let inbox: Inbox = Arc::new(Mutex::new(vec![tx]));
                        let abort =
                            spawn_dialer(&self.inner, node_id, host, port, inbox.clone(), 1);
                        e.insert(Slot::Dialing {
                            inbox,
                            abort_on_drop: abort,
                        });
                    }
                },
                Entry::Vacant(v) => {
                    let inbox: Inbox = Arc::new(Mutex::new(vec![tx]));
                    let abort = spawn_dialer(&self.inner, node_id, host, port, inbox.clone(), 0);
                    v.insert(Slot::Dialing {
                        inbox,
                        abort_on_drop: abort,
                    });
                }
            }

            rx
        };

        // The dialer only sends on success. `Err` here means the
        // oneshot sender was dropped without a value — the dialer task
        // was aborted (via `close()` or metadata pruning) or the slot
        // was replaced out from under us. Permanent dial failures do
        // NOT surface here; they keep the waiter parked across retry
        // cycles.
        match rx.await {
            Ok(client) => Ok(client),
            Err(_) => Err(Error::ProtocolError("broker dial cancelled".into())),
        }
    }

    pub async fn any_broker(&self) -> Result<BrokerClient> {
        // Fast path: find the first connection that is still alive.
        {
            let metadata = self.inner.metadata.load();
            let map = self.inner.connections.lock().unwrap();
            for (broker_id, slot) in map.iter() {
                if let Slot::Resolved(broker) = slot {
                    if !broker.is_shutdown() && metadata.brokers.contains_key(broker_id) {
                        return Ok(broker.clone());
                    }
                }
            }
        }

        // Otherwise route through `broker(id)` for each known broker —
        // that path handles dialing, gating, and slot insertion uniformly.
        // Iterate over ids so one dead broker (gated by its dialer's
        // backoff sleep) doesn't starve callers when another id is
        // healthy.
        let ids: Vec<BrokerId> = {
            let snap = self.inner.metadata.load();
            snap.brokers.keys().copied().collect()
        };
        if ids.is_empty() {
            return Err(Error::ProtocolError("no known brokers".into()));
        }

        let mut last_err = None;
        for id in ids {
            // Skip any broker whose slot is currently `Dialing`: its
            // backoff loop is running and `broker(id)` would park us on
            // a potentially long sleep. A healthy sibling is worth more
            // to `any_broker`'s callers than a gated one.
            {
                let map = self.inner.connections.lock().unwrap();
                if matches!(map.get(&id), Some(Slot::Dialing { .. })) {
                    continue;
                }
            }
            match self.broker(id).await {
                Ok(c) => return Ok(c),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| Error::ProtocolError("no reachable brokers".into())))
    }

    pub async fn refresh_metadata(&self) -> Result<MetadataResponse> {
        let broker = self.any_broker().await?;
        let metadata = broker.fetch_metadata().await?;

        let mut brokers = HashMap::with_capacity(metadata.brokers.len());
        for b in &metadata.brokers {
            let (host, port) = resolve_address(
                &self.inner.address_resolver,
                b.node_id,
                b.host.as_str(),
                b.port,
            )?;
            brokers.insert(b.node_id, BrokerInfo { host, port });
        }

        self.apply_metadata_snapshot(MetadataSnapshot {
            controller_id: metadata.controller_id,
            brokers,
        });

        Ok(metadata)
    }

    /// Clean up connections & replace metadata. Pruned slots drop their
    /// `Slot::Dialing` variant (if any), which aborts the background
    /// dialer — callers parked on its inbox wake with a closed-channel
    /// error and return promptly.
    fn apply_metadata_snapshot(&self, snap: MetadataSnapshot) {
        {
            let mut conns = self.inner.connections.lock().unwrap();
            conns.retain(|id, _| snap.brokers.contains_key(id));
        }
        // Atomic publish: concurrent readers either see the old or the new
        // snapshot, never a torn view.
        self.inner.metadata.store(Arc::new(snap));
    }

    /// Test-only: number of slots currently held in the connection cache.
    /// Used to assert that bogus `broker(id)` calls don't leak empty slots.
    #[cfg(test)]
    pub(crate) fn connection_slot_count(&self) -> usize {
        self.inner.connections.lock().unwrap().len()
    }

    /// Test-only: replace the published metadata snapshot with a synthetic
    /// one, going through the same prune-then-publish helper that
    /// `refresh_metadata` uses. Lets tests simulate a broker disappearing
    /// from the cluster without orchestrating a real container shutdown.
    #[cfg(test)]
    pub(crate) fn replace_metadata_for_test(
        &self,
        controller_id: BrokerId,
        brokers: Vec<(BrokerId, String, u16)>,
    ) {
        let mut map = HashMap::with_capacity(brokers.len());
        for (id, host, port) in brokers {
            map.insert(id, BrokerInfo { host, port });
        }
        self.apply_metadata_snapshot(MetadataSnapshot {
            controller_id,
            brokers: map,
        });
    }

    /// Test-only: does the connection cache currently have a slot for this id?
    #[cfg(test)]
    pub(crate) fn has_connection_slot(&self, node_id: BrokerId) -> bool {
        self.inner
            .connections
            .lock()
            .unwrap()
            .contains_key(&node_id)
    }

    pub fn controller_id(&self) -> BrokerId {
        self.inner.metadata.load().controller_id
    }

    /// Signal shutdown on every pooled broker connection and clear the pool.
    ///
    /// Idempotent. After `close()` returns, background read/write
    /// tasks are signalled but exit asynchronously.
    pub fn close(&self) {
        let mut map = self.inner.connections.lock().unwrap();
        for (_id, slot) in map.drain() {
            match slot {
                Slot::Resolved(broker) => broker.shutdown(),
                Slot::Dialing { abort_on_drop, .. } => {
                    // The following drop will drop the background retry task.
                    drop(abort_on_drop);
                },
            }
        }
    }

    /// Returns an [`AdminClient`] handle for cluster-management RPCs
    /// (topic creation, config changes, etc.). Cheap to call — the returned
    /// handle shares this `Client`'s connection pool and metadata cache.
    pub fn admin(&self) -> AdminClient {
        AdminClient::new(self.clone())
    }
}

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
fn spawn_dialer(
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

fn resolve_address(
    resolver: &Option<AddressResolver>,
    node_id: BrokerId,
    host: &str,
    port: i32,
) -> Result<(String, u16)> {
    match resolver {
        Some(f) => f(node_id, host, port),
        None => {
            let Ok(port) = u16::try_from(port) else {
                return Err(Error::ProtocolError(format!(
                    "Cannot convert {port} port number to u16"
                )));
            };
            Ok((host.to_owned(), port))
        }
    }
}
