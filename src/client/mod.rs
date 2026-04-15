use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use kafka_protocol::messages::{BrokerId, MetadataResponse};
use tokio::sync::oneshot;

use crate::admin::AdminClient;
use crate::broker::{Auth, BrokerClient};
use crate::config::{Config, Security};
use crate::connection::Connection;
use crate::error::{Error, Result};

mod retry;

use retry::{ConnectionMap, Inbox, Slot, spawn_dialer};
#[cfg(test)]
pub(crate) use retry::next_backoff;

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
