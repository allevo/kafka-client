use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::{BrokerId, CreateTopicsResponse, MetadataResponse};
use tokio::sync::OnceCell;

use crate::client::BrokerClient;
use crate::config::Config;
use crate::connection::{Auth, Connection, Security};
use crate::error::{Error, Result};

/// Translates broker addresses from metadata into actual connectable addresses.
///
/// Receives `(node_id, advertised_host, advertised_port)` and returns
/// `(actual_host, actual_port)`. Useful when brokers sit behind NAT,
/// Docker port mapping, or a proxy.
type AddressResolver = Arc<dyn Fn(BrokerId, &str, i32) -> (String, u16) + Send + Sync>;

struct BrokerInfo {
    host: String,
    port: u16,
}

/// Immutable view of the cluster, published atomically via `ArcSwap`.
///
/// Readers do `inner.metadata.load()` and walk this snapshot without locking;
/// writers (only `refresh_metadata`) build a fresh snapshot and `store()` it.
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

    metadata: ArcSwap<MetadataSnapshot>,

    /// Per-broker connection slots.
    connections: Mutex<HashMap<BrokerId, Arc<OnceCell<BrokerClient>>>>,
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
        resolver: impl Fn(BrokerId, &str, i32) -> (String, u16) + Send + Sync + 'static,
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
                    let client = BrokerClient::new(conn, auth.clone()).await?;
                    let metadata = client.fetch_metadata().await?;

                    let mut brokers = HashMap::with_capacity(metadata.brokers.len());
                    let mut bootstrap_node_id = None;

                    for broker in &metadata.brokers {
                        let (host, port) = resolve_address(
                            &address_resolver,
                            broker.node_id,
                            broker.host.as_str(),
                            broker.port,
                        );
                        if bootstrap_node_id.is_none()
                            && host == config.host
                            && port == config.port
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
                    let mut connections: HashMap<BrokerId, Arc<OnceCell<BrokerClient>>> =
                        HashMap::new();
                    if let Some(node_id) = bootstrap_node_id {
                        connections.insert(node_id, Arc::new(OnceCell::new_with(Some(client))));
                    }

                    let inner = ClientInner {
                        security,
                        auth,
                        max_response_size: config.max_response_size,
                        address_resolver,
                        metadata: ArcSwap::new(Arc::new(MetadataSnapshot {
                            controller_id: metadata.controller_id,
                            brokers,
                        })),
                        connections: Mutex::new(connections),
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

        Err(last_err.unwrap_or_else(|| {
            Error::ProtocolError("no bootstrap brokers provided".into())
        }))
    }

    pub async fn controller(&self) -> Result<BrokerClient> {
        let id = self.inner.metadata.load().controller_id;
        self.broker(id).await
    }

    pub async fn broker(&self, node_id: BrokerId) -> Result<BrokerClient> {
        // Fast path: get the connection if:
        // - it is still alive
        // - the metadata contains it
        {
            let metadata = self.inner.metadata.load();
            let mut map = self.inner.connections.lock().unwrap();
            if let Some(cell) = map.get(&node_id) {
                if let Some(client) = cell.get() {
                    if !client.is_shutdown() && metadata.brokers.contains_key(&node_id) {
                        return Ok(client.clone());
                    }
                    // Dead or no longer in metadata — evict so the cold path
                    // either redials (shutdown) or rejects (unknown id).
                    map.remove(&node_id);
                }
            }
        }

        // Cold path: No live brokerclient is found
        // - Get metadata
        // - Replace (mainly insert) a new OnceCell to the connections map
        // - Init connection   

        let (host, port) = {
            let snap = self.inner.metadata.load();
            let info = snap.brokers.get(&node_id).ok_or_else(|| {
                Error::ProtocolError(format!("unknown broker node_id: {}", node_id.0))
            })?;
            (info.host.clone(), info.port)
        };

        let cell = {
            let mut map = self.inner.connections.lock().unwrap();

            // During a lock we return or insert the arc by node_id
            map.entry(node_id)
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };

        let security = self.inner.security.clone();
        let auth = self.inner.auth.clone();
        let max_response_size = self.inner.max_response_size;
        let client = cell
            .get_or_try_init(|| async move {
                let config = Config::new(&host, port).with_max_response_size(max_response_size);
                let conn = Connection::connect(&config, security).await?;
                BrokerClient::new(conn, auth).await
            })
            .await?;
        Ok(client.clone())
    }

    pub async fn any_broker(&self) -> Result<BrokerClient> {
        // Fast path: find the first connection that is still alive
        {
            let metadata = self.inner.metadata.load();
            let map = self.inner.connections.lock().unwrap();
            for (broker_id, cell) in &*map {
                if let Some(client) = cell.get() {
                    if !client.is_shutdown() && metadata.brokers.contains_key(broker_id) {
                        return Ok(client.clone());
                    }
                }
            }
        }

        // Otherwise route through `broker(id)` for the first known broker —
        // that path handles dialing and slot insertion uniformly.
        let id = {
            let snap = self.inner.metadata.load();
            *snap
                .brokers
                .keys()
                .next()
                .ok_or_else(|| Error::ProtocolError("no known brokers".into()))?
        };
        self.broker(id).await
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
            );
            brokers.insert(b.node_id, BrokerInfo { host, port });
        }

        self.apply_metadata_snapshot(MetadataSnapshot {
            controller_id: metadata.controller_id,
            brokers,
        });

        Ok(metadata)
    }

    /// Clean up connections & replace metadata
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
        self.inner.connections.lock().unwrap().contains_key(&node_id)
    }

    pub async fn create_topics(
        &self,
        topics: Vec<CreatableTopic>,
        timeout_ms: i32,
    ) -> Result<CreateTopicsResponse> {
        // CreateTopics is routed to the controller.
        // Since Kafka 2.4 (KIP-590) any broker will forward admin requests to the
        // controller, but targeting the controller directly is still canonical in
        // the other clients.
        let controller = self.controller().await?;
        controller.create_topics(topics, timeout_ms).await
    }

    pub fn controller_id(&self) -> BrokerId {
        self.inner.metadata.load().controller_id
    }
}

fn resolve_address(
    resolver: &Option<AddressResolver>,
    node_id: BrokerId,
    host: &str,
    port: i32,
) -> (String, u16) {
    match resolver {
        Some(f) => f(node_id, host, port),
        None => (host.to_owned(), port as u16),
    }
}
