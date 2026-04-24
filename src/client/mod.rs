use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwap;
use kafka_protocol::ResponseError;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::{ApiKey, BrokerId, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion};
use tokio::sync::oneshot;

use crate::admin::AdminClient;
use crate::broker::{Auth, BrokerClient, ResponseFuture};
use crate::config::{Config, Security};
use crate::connection::Connection;
use crate::error::{Error, Result, RetryAction};

/// Which broker `Client::send` should route a request to.
///
/// Mirrors Java's `NodeProvider` abstraction: admin/control-plane RPCs
/// describe the kind of node they need, and the dispatcher picks one.
pub enum NodeTarget {
    Controller,
    AnyBroker,
    Broker(BrokerId),
}

/// Per-call overrides for the client-side retry loop that governs every
/// RPC. Falls back to [`crate::Config`].
#[derive(Debug, Default, Clone)]
pub struct CallOptions {
    timeout: Option<Duration>,
    retries: Option<u32>,
    retry_backoff: Option<Duration>,
    retry_backoff_max: Option<Duration>,
}

impl CallOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the client-side deadline for this call. Corresponds to
    /// `Config::api_timeout` and caps the total retry-loop duration.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Override the maximum number of retry attempts after the first
    /// send. `0` disables retries for this call only. Corresponds to
    /// `Config::retries`.
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Override the base backoff between retry attempts. Corresponds to
    /// `Config::retry_backoff`.
    pub fn with_retry_backoff(mut self, base: Duration) -> Self {
        self.retry_backoff = Some(base);
        self
    }

    /// Override the cap on the exponential retry backoff. Corresponds to
    /// `Config::retry_backoff_max`.
    pub fn with_retry_backoff_max(mut self, max: Duration) -> Self {
        self.retry_backoff_max = Some(max);
        self
    }

    pub(crate) fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    pub(crate) fn retries(&self) -> Option<u32> {
        self.retries
    }

    pub(crate) fn retry_backoff(&self) -> Option<Duration> {
        self.retry_backoff
    }

    pub(crate) fn retry_backoff_max(&self) -> Option<Duration> {
        self.retry_backoff_max
    }
}

/// Handle for an in-flight `Client::send`-layer request whose response
/// has not yet arrived, bound to the `api_timeout` / `request_timeout`
/// deadline of the call that produced it.
///
/// Wraps a [`ResponseFuture`] so the underlying oneshot and decoding stay
/// on the caller's task (pipelining-friendly), and polls a `Sleep` on the
/// same deadline so the response-wait half stays bounded even when the
/// caller holds this future across task boundaries.
///
/// On deadline expiry the broker connection is torn down via
/// [`BrokerClient::shutdown`], matching the retry-loop contract: the next
/// `Client::send` attempt sees a dead `Slot::Resolved` and redials.
pub struct ClientResponseFuture<Resp> {
    inner: ResponseFuture<Resp>,
    // `Pin<Box<Sleep>>` so the whole struct stays `Unpin` — `Sleep` is
    // `!Unpin` on its own, which would force manual pin projection here.
    sleep: Pin<Box<tokio::time::Sleep>>,
    broker: BrokerClient,
}

impl<Resp> ClientResponseFuture<Resp>
where
    Resp: Decodable + HeaderVersion,
{
    pub fn new(
        inner: ResponseFuture<Resp>,
        deadline: tokio::time::Instant,
        broker: BrokerClient,
    ) -> Self {
        Self {
            inner,
            sleep: Box::pin(tokio::time::sleep_until(deadline)),
            broker,
        }
    }
}

impl<Resp> Future for ClientResponseFuture<Resp>
where
    Resp: Decodable + HeaderVersion,
{
    type Output = Result<Resp>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        // Poll the response first: if it's ready at the same tick as the
        // deadline, the response wins.
        if let Poll::Ready(r) = Pin::new(&mut this.inner).poll(cx) {
            return Poll::Ready(r);
        }

        if this.sleep.as_mut().poll(cx).is_ready() {
            // Tear down the broker connection so the retry loop's next
            // `Client::broker(id)` call evicts this slot and redials.
            this.broker.shutdown();
            return Poll::Ready(Err(Error::RequestTimeout(
                "in-flight request exceeded request_timeout".into(),
            )));
        }

        Poll::Pending
    }
}

mod retry;

pub(crate) use retry::next_backoff;
#[cfg(test)]
pub(crate) use retry::test_hooks;
use retry::{ConnectionMap, Inbox, Slot, spawn_dialer};

/// Translates broker addresses from metadata into actual connectable addresses.
///
/// Receives `(node_id, advertised_host, advertised_port)` and returns
/// `(actual_host, actual_port)`. Useful when brokers sit behind NAT,
/// Docker port mapping, or a proxy.
type AddressResolver = Arc<dyn Fn(BrokerId, &str, i32) -> Result<(String, u16)> + Send + Sync>;

#[derive(Clone)]
struct BrokerInfo {
    host: String,
    port: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct PartitionId(pub i32);

#[derive(Clone, Copy, Debug)]
pub(crate) struct PartitionMetadata {
    pub leader: BrokerId,
    pub leader_epoch: i32,
}

#[derive(Clone, Debug)]
pub(crate) struct TopicMetadata {
    /// Sorted ascending by partition id.
    pub partitions: Vec<(PartitionId, PartitionMetadata)>,
    /// `Some` when the broker returned a topic-level error code.
    /// NB: Callers distinguish "not cached" (None from `topic_metadata`) from
    /// "known but errored" (Some(TopicMetadata { error: Some(_), .. })).
    pub error: Option<ResponseError>,
}

/// Immutable view of the cluster. This is the cached metadata response.
struct MetadataSnapshot {
    controller_id: BrokerId,
    brokers: HashMap<BrokerId, BrokerInfo>,
    /// Known TopicMetadata per name.
    ///
    ///
    /// NB: Absence of a key means "not fetched yet", not "doesn't exist".
    topics: HashMap<TopicName, TopicMetadata>,
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

    connection_setup_timeout: Option<Duration>,
    connections_max_idle: Option<Duration>,

    reconnect_backoff: Duration,
    reconnect_backoff_max: Duration,

    request_timeout: Duration,
    api_timeout: Duration,
    retries: u32,
    retry_backoff: Duration,
    retry_backoff_max: Duration,

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

                    let brokers = resolve_brokers(&address_resolver, &metadata.brokers)?;
                    let topics = build_topic_map(&metadata.topics);

                    let mut connections: HashMap<BrokerId, Slot> = HashMap::new();

                    // We already used bootstrap node, so add it to connections
                    let bootstrap_node_id = brokers.iter().find_map(|(id, info)| {
                        (info.host == config.host && info.port == config.port).then_some(*id)
                    });
                    if let Some(node_id) = bootstrap_node_id {
                        connections.insert(node_id, Slot::Resolved(broker));
                    }

                    let inner = ClientInner {
                        security,
                        auth,
                        max_response_size: config.max_response_size,
                        address_resolver,
                        connection_setup_timeout: config.connection_setup_timeout,
                        connections_max_idle: config.connections_max_idle,
                        reconnect_backoff: config.reconnect_backoff,
                        reconnect_backoff_max: config.reconnect_backoff_max,
                        request_timeout: config.request_timeout,
                        api_timeout: config.api_timeout,
                        retries: config.retries,
                        retry_backoff: config.retry_backoff,
                        retry_backoff_max: config.retry_backoff_max,
                        metadata: ArcSwap::new(Arc::new(MetadataSnapshot {
                            controller_id: metadata.controller_id,
                            brokers,
                            topics,
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

        Err(last_err.unwrap_or_else(|| Error::Config("no bootstrap brokers provided".into())))
    }

    pub async fn controller(&self) -> Result<BrokerClient> {
        let id = self.inner.metadata.load().controller_id;
        self.broker(id).await
    }

    /// Dispatch a typed Kafka request: pick a broker for `target`,
    /// negotiate the wire version against that broker, send the request,
    /// and decode the response.
    ///
    /// `request` is owned by the retry loop and encoded by reference, so
    /// retries reuse the same value without cloning. Per-attempt version
    /// negotiation is applied at encode time. Callers that need to rebuild
    /// version-gated fields against each attempt's negotiated version are
    /// not served by this entry point; add a separate builder-based method
    /// when such a caller appears.
    pub async fn send<Req, Resp>(
        &self,
        target: NodeTarget,
        api_key: ApiKey,
        max_version: i16,
        request: Req,
        opts: &CallOptions,
    ) -> Result<Resp>
    where
        Req: Encodable + HeaderVersion + Send + 'static,
        Resp: Decodable + HeaderVersion,
    {
        let api_timeout = opts.timeout().unwrap_or(self.inner.api_timeout);
        let max_retries = opts.retries().unwrap_or(self.inner.retries);
        let retry_backoff = opts.retry_backoff().unwrap_or(self.inner.retry_backoff);
        let retry_backoff_max = opts
            .retry_backoff_max()
            .unwrap_or(self.inner.retry_backoff_max);

        let api_deadline = tokio::time::Instant::now() + api_timeout;
        let mut attempt: u32 = 0;

        loop {
            let now = tokio::time::Instant::now();
            if now >= api_deadline {
                return Err(Error::RequestTimeout("api_timeout exhausted".into()));
            }

            let per_attempt = (now + self.inner.request_timeout).min(api_deadline);

            match self
                .send_once(per_attempt, &target, api_key, max_version, &request)
                .await
            {
                Ok(resp) => return Ok(resp),
                Err(e) => {
                    if attempt >= max_retries {
                        return Err(e);
                    }
                    match e.classify() {
                        RetryAction::Fatal | RetryAction::Abortable => return Err(e),
                        RetryAction::RefreshMetadata => {
                            // Refresh metadata almost ignoring errors
                            match tokio::time::timeout_at(api_deadline, self.refresh_metadata())
                                .await
                            {
                                Ok(Ok(_)) => {}
                                Ok(Err(refresh_err)) if refresh_err.is_fatal() => {
                                    return Err(refresh_err);
                                }
                                Ok(Err(_)) => {}
                                // timeout
                                Err(_) => {}
                            }
                        }
                        RetryAction::Retry => {}
                    }
                    attempt = attempt.saturating_add(1);

                    let backoff = next_backoff(attempt, retry_backoff, retry_backoff_max);
                    let sleep_until = (tokio::time::Instant::now() + backoff).min(api_deadline);
                    tokio::time::sleep_until(sleep_until).await;
                }
            }
        }
    }

    /// Single attempt of `send`.
    async fn send_once<Req, Resp>(
        &self,
        deadline: tokio::time::Instant,
        target: &NodeTarget,
        api_key: ApiKey,
        max_version: i16,
        request: &Req,
    ) -> Result<Resp>
    where
        Req: Encodable + HeaderVersion + Send + 'static,
        Resp: Decodable + HeaderVersion,
    {
        // Acquisition phase — covers controller()/any_broker()/broker(id).
        let broker = match tokio::time::timeout_at(deadline, async {
            match target {
                NodeTarget::Controller => self.controller().await,
                NodeTarget::AnyBroker => self.any_broker().await,
                NodeTarget::Broker(id) => self.broker(*id).await,
            }
        })
        .await
        {
            Ok(r) => r?,
            Err(_) => {
                return Err(Error::RequestTimeout(
                    "broker acquisition exceeded request_timeout".into(),
                ));
            }
        };

        let version = broker.negotiate_version(api_key, max_version)?;

        // This timeout takes only the half side: it consider only the "write",
        // moving the "read" part to the `ClientResponseFuture::poll`.
        let response_fut =
            match tokio::time::timeout_at(deadline, broker.send_request(api_key, version, request))
                .await
            {
                Ok(r) => r?,
                Err(_) => {
                    // Intentionally do NOT manually evict the slot:
                    // `Client::broker` will auto-evict it next retry
                    broker.shutdown();
                    return Err(Error::RequestTimeout(
                        "in-flight request exceeded request_timeout".into(),
                    ));
                }
            };

        ClientResponseFuture::new(response_fut, deadline, broker).await
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
                Error::NoBrokerAvailable(format!("unknown broker node_id: {}", node_id.0))
            })?;
            (info.host.clone(), info.port)
        };

        let rx = {
            let mut map = self.inner.connections.lock().unwrap();

            // Fast path: live cached client.
            if let Some(Slot::Resolved(c)) = map.get(&node_id)
                && !c.is_shutdown()
            {
                return Ok(c.clone());
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
            Err(_) => Err(Error::NoBrokerAvailable("broker dial cancelled".into())),
        }
    }

    pub async fn any_broker(&self) -> Result<BrokerClient> {
        // Fast path: find the first connection that is still alive.
        {
            let metadata = self.inner.metadata.load();
            let map = self.inner.connections.lock().unwrap();
            for (broker_id, slot) in map.iter() {
                if let Slot::Resolved(broker) = slot
                    && !broker.is_shutdown()
                    && metadata.brokers.contains_key(broker_id)
                {
                    return Ok(broker.clone());
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
            return Err(Error::NoBrokerAvailable("no known brokers".into()));
        }

        let mut last_err = None;
        let mut skipped_dialing: Option<BrokerId> = None;

        for id in ids {
            // Skip any broker whose slot is currently `Dialing`: its
            // backoff loop is running and `broker(id)` would park us on
            // a potentially long sleep. A healthy sibling is worth more
            // to `any_broker`'s callers than a gated one.
            {
                let map = self.inner.connections.lock().unwrap();
                if matches!(map.get(&id), Some(Slot::Dialing { .. })) {
                    skipped_dialing.get_or_insert(id);
                    continue;
                }
            }
            match self.broker(id).await {
                Ok(c) => return Ok(c),
                Err(e) => last_err = Some(e),
            }
        }

        // Every non-dialing broker failed (or there were none). Rather
        // than returning an immediate error, park on one in-progress
        // dialer — this covers the all-brokers-dialing scenario
        // (cluster startup, full reconnection after partition).
        if let Some(id) = skipped_dialing {
            tracing::debug!(broker_id = %id.0, "all brokers dialing, parking on in-progress dial");
            match self.broker(id).await {
                Ok(c) => return Ok(c),
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err.unwrap_or_else(|| Error::NoBrokerAvailable("no reachable brokers".into())))
    }

    /// Return `TopicMetadata` or None in case the topic has never been seen.
    ///
    /// NB: `TopicMetadata` can contain `error: Some(_)` for topics the broker reported an error for
    /// (e.g. UnknownTopicOrPartition) so callers can distinguish
    /// "not cached" from "known but errored".
    pub(crate) fn topic_metadata(&self, topic: &TopicName) -> Option<TopicMetadata> {
        self.inner.metadata.load().topics.get(topic).cloned()
    }

    /// Refresh the metadata for the given topic
    pub(crate) async fn refresh_topics(&self, topics: &[TopicName]) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        let request = MetadataRequest::default().with_topics(Some(
            topics
                .iter()
                .cloned()
                .map(|name| MetadataRequestTopic::default().with_name(Some(name)))
                .collect(),
        ));

        let resp: MetadataResponse = self
            .send(
                NodeTarget::AnyBroker,
                ApiKey::Metadata,
                1,
                request,
                // Disable retry to avoid loops on error
                &CallOptions::new().with_retries(0),
            )
            .await?;

        let new_topics = build_topic_map(&resp.topics);
        let new_brokers = resolve_brokers(&self.inner.address_resolver, &resp.brokers)?;

        // NB: perform this holding the lock is important
        // See apply_metadata_snapshot
        let mut conns = self.inner.connections.lock().unwrap();
        let current = self.inner.metadata.load();
        let mut merged_topics = current.topics.clone();
        for (name, meta) in &new_topics {
            merged_topics.insert(name.clone(), meta.clone());
        }
        let new_snap = Arc::new(MetadataSnapshot {
            controller_id: resp.controller_id,
            brokers: new_brokers,
            topics: merged_topics,
        });
        self.inner.metadata.store(new_snap.clone());
        prune_map(&mut conns, &new_snap);
        drop(conns);

        Ok(())
    }

    pub async fn refresh_metadata(&self) -> Result<MetadataResponse> {
        let broker = self.any_broker().await?;
        let metadata = broker.fetch_metadata().await?;

        let brokers = resolve_brokers(&self.inner.address_resolver, &metadata.brokers)?;
        let topics = build_topic_map(&metadata.topics);

        self.apply_metadata_snapshot(MetadataSnapshot {
            controller_id: metadata.controller_id,
            brokers,
            topics,
        });

        Ok(metadata)
    }

    /// Replace metadata and clean up connections
    fn apply_metadata_snapshot(&self, snap: MetadataSnapshot) {
        // Concurrent access to connection and metadata can create
        // inconsistency from the readers point of view.
        // So, it is important to publish the metadata inside the connections lock
        // so any observer that takes the lock sees metadata and map consistently
        let mut conns = self.inner.connections.lock().unwrap();
        let snap = Arc::new(snap);
        self.inner.metadata.store(snap.clone());
        prune_map(&mut conns, &snap);
        drop(conns);
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
        topics: Vec<(TopicName, TopicMetadata)>,
    ) {
        let mut broker_map = HashMap::with_capacity(brokers.len());
        for (id, host, port) in brokers {
            broker_map.insert(id, BrokerInfo { host, port });
        }
        let mut topic_map = HashMap::with_capacity(topics.len());
        for (name, meta) in topics {
            topic_map.insert(name, meta);
        }
        self.apply_metadata_snapshot(MetadataSnapshot {
            controller_id,
            brokers: broker_map,
            topics: topic_map,
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

    /// Test-only: remove a `Slot::Dialing` entry from the map without
    /// aborting its background dialer task. Simulates the effect of
    /// `close()` on the map while keeping the dialer alive so a test can
    /// verify it handles the slot-gone case.
    #[cfg(test)]
    pub(crate) fn remove_slot_without_abort(&self, node_id: BrokerId) {
        let mut map = self.inner.connections.lock().unwrap();
        if let Some(Slot::Dialing { abort_on_drop, .. }) = map.remove(&node_id) {
            // Intentionally leak the AbortOnDrop so the dialer task is
            // NOT cancelled — we want it to reach the slot_is_ours check.
            std::mem::forget(abort_on_drop);
        }
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
                }
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

/// Evict any slot whose broker id is not in `snap.brokers`.
///
/// Pruned `Slot::Resolved` entries are signalled via `BrokerClient::shutdown`;
/// pruned `Slot::Dialing` entries drop their `AbortOnDrop`, cancelling
/// the background dialer.
///
/// NB: Caller has to hold the connections lock.
/// See `Client::apply_metadata_snapshot`
fn prune_map(conns: &mut HashMap<BrokerId, Slot>, snap: &MetadataSnapshot) {
    conns.retain(|id, slot| {
        if snap.brokers.contains_key(id) {
            return true;
        }
        match slot {
            Slot::Resolved(broker) => broker.shutdown(),
            // AbortOnDrop fires when the slot is dropped below.
            Slot::Dialing { .. } => {}
        }
        false
    });
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
                return Err(Error::Config(format!(
                    "Cannot convert {port} port number to u16"
                )));
            };
            Ok((host.to_owned(), port))
        }
    }
}

fn resolve_brokers(
    resolver: &Option<AddressResolver>,
    brokers: &[kafka_protocol::messages::metadata_response::MetadataResponseBroker],
) -> Result<HashMap<BrokerId, BrokerInfo>> {
    let mut out = HashMap::with_capacity(brokers.len());
    for b in brokers {
        let (host, port) = resolve_address(resolver, b.node_id, b.host.as_str(), b.port)?;
        out.insert(b.node_id, BrokerInfo { host, port });
    }
    Ok(out)
}

fn build_topic_map(topics: &[MetadataResponseTopic]) -> HashMap<TopicName, TopicMetadata> {
    let mut out = HashMap::with_capacity(topics.len());
    for t in topics {
        // Kafka Protocol allows `name == None` ("no name returned")
        // In this case, we discard them because the client cannot refers to them
        let Some(name) = t.name.clone() else {
            continue;
        };

        let mut partitions: Vec<(PartitionId, PartitionMetadata)> = t
            .partitions
            .iter()
            .map(|p| {
                (
                    PartitionId(p.partition_index),
                    PartitionMetadata {
                        leader: p.leader_id,
                        leader_epoch: p.leader_epoch,
                    },
                )
            })
            .collect();
        partitions.sort_by_key(|(id, _)| *id);

        let error = ResponseError::try_from_code(t.error_code);

        out.insert(name, TopicMetadata { partitions, error });
    }
    out
}

/// Find `PartitionMetadata` by `PartitionId`.
pub(crate) fn find_partition(
    partitions: &[(PartitionId, PartitionMetadata)],
    id: PartitionId,
) -> Option<&PartitionMetadata> {
    debug_assert!(partitions.is_sorted_by_key(|(p_id, _)| *p_id));

    // Accordingly with Kafka Protocol, the partition id is i32
    // Anyway, all partition ids are >= 0.
    if id.0 < 0 {
        return None;
    }

    let idx = id.0 as usize;
    if let Some((pid, meta)) = partitions.get(idx)
        && *pid == id
    {
        return Some(meta);
    }
    partitions
        .binary_search_by_key(&id, |(pid, _)| *pid)
        .ok()
        .map(|i| &partitions[i].1)
}

#[cfg(test)]
mod find_partition_tests {
    use super::*;

    fn entry(id: i32, leader: i32) -> (PartitionId, PartitionMetadata) {
        (
            PartitionId(id),
            PartitionMetadata {
                leader: BrokerId(leader),
                leader_epoch: 0,
            },
        )
    }

    #[test]
    fn contiguous_hits_positional_fast_path() {
        let parts = vec![entry(0, 10), entry(1, 11), entry(2, 12)];
        assert_eq!(
            find_partition(&parts, PartitionId(0)).unwrap().leader,
            BrokerId(10)
        );
        assert_eq!(
            find_partition(&parts, PartitionId(2)).unwrap().leader,
            BrokerId(12)
        );
    }

    #[test]
    fn non_contiguous_falls_back_to_binary_search() {
        // Partitions 0, 1, 3 — probing position 3 lands on index 3 which
        // is out of bounds, so the binary search takes over.
        let parts = vec![entry(0, 10), entry(1, 11), entry(3, 13)];
        assert_eq!(
            find_partition(&parts, PartitionId(3)).unwrap().leader,
            BrokerId(13)
        );
        assert!(find_partition(&parts, PartitionId(2)).is_none());
    }

    #[test]
    fn out_of_range_and_negative_ids_return_none() {
        let parts = vec![entry(0, 10), entry(1, 11)];
        assert!(find_partition(&parts, PartitionId(5)).is_none());
        // Negative id.0 wraps to a huge usize and misses the fast path;
        // binary search then reports absence.
        assert!(find_partition(&parts, PartitionId(-1)).is_none());
    }
}
