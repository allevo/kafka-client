use std::time::{Duration, Instant};

use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::metadata_response::MetadataResponseTopic;
use kafka_protocol::messages::{
    ApiKey, BrokerId, CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest,
    CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse, DescribeClusterRequest,
    DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, MetadataRequest,
    MetadataResponse, TopicName,
};

use crate::client::{CallOptions, Client, NodeTarget};
use crate::error::{Error, PropagationDirection, Result};

// Kafka error codes for the per-topic results we treat as "the topic
// definitively exists (or doesn't) on the controller". Anything else is a
// real failure and we don't wait on it.
//
// 36 = TOPIC_ALREADY_EXISTS, 3 = UNKNOWN_TOPIC_OR_PARTITION.
const ERR_TOPIC_ALREADY_EXISTS: i16 = 36;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;

/// Options for [`AdminClient::create_topics`].
///
/// Default: `wait_for_propagation = true`, `broker_operation_timeout = None`
/// (the call gets [`DEFAULT_BROKER_OPERATION_TIMEOUT`]).
#[derive(Debug, Clone)]
pub struct CreateTopicsOptions {
    /// Wall-clock budget for the whole call:
    ///   (a) the wire `timeout_ms` sent to the controller, and
    ///   (b) the deadline for the post-RPC propagation wait when
    ///       `wait_for_propagation` is true.
    /// `None` falls back to [`DEFAULT_BROKER_OPERATION_TIMEOUT`].
    pub broker_operation_timeout: Option<Duration>,

    /// When `true` (default), after the controller acknowledges the request
    /// the call polls every broker until each created topic surfaces in its
    /// MetadataCache with elected partition leaders. When `false`, return
    /// as soon as the controller responds — the caller accepts that an
    /// immediate produce/consume against a sibling broker may race the
    /// per-broker metadata replay.
    pub wait_for_propagation: bool,
}

impl Default for CreateTopicsOptions {
    fn default() -> Self {
        Self {
            broker_operation_timeout: None,
            wait_for_propagation: true,
        }
    }
}

/// Options for [`AdminClient::delete_topics`].
///
/// Default: `wait_for_propagation = true`, `broker_operation_timeout = None`
/// (the call gets [`DEFAULT_BROKER_OPERATION_TIMEOUT`]).
#[derive(Debug, Clone)]
pub struct DeleteTopicsOptions {
    /// Wall-clock budget for the whole call: wire `timeout_ms` plus the
    /// post-RPC propagation wait when `wait_for_propagation` is true.
    pub broker_operation_timeout: Option<Duration>,

    /// When `true` (default), poll every broker until each deleted topic
    /// disappears from its MetadataCache.
    pub wait_for_propagation: bool,
}

impl Default for DeleteTopicsOptions {
    fn default() -> Self {
        Self {
            broker_operation_timeout: None,
            wait_for_propagation: true,
        }
    }
}

/// Admin surface for cluster-management RPCs.
///
/// Administrative operations live here.
/// An `AdminClient` is a thin, cheap-to-clone handle over an existing [`Client`];
/// it reuses the parent's connection pool and metadata cache — it does not open its own.
#[derive(Clone)]
pub struct AdminClient {
    client: Client,
}

impl AdminClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Create one or more topics. Routed to the controller.
    ///
    /// `options.broker_operation_timeout` is the wall-clock budget for the
    /// whole call: it is sent to the controller as the wire `timeout_ms`
    /// and also bounds the post-RPC propagation wait when
    /// `options.wait_for_propagation` is true (the default). It is
    /// independent of the client-side per-attempt deadline in `opts`.
    ///
    /// When `wait_for_propagation` is true, the call polls every broker
    /// after the controller acknowledges the request and only returns once
    /// each created topic surfaces with elected partition leaders. This
    /// closes the KRaft race where a Producer would otherwise hit
    /// `UNKNOWN_TOPIC_OR_PARTITION` against a sibling whose MetadataCache
    /// replay was still in flight.
    pub async fn create_topics(
        &self,
        topics: Vec<CreatableTopic>,
        options: CreateTopicsOptions,
        opts: CallOptions,
    ) -> Result<CreateTopicsResponse> {
        let budget = options
            .broker_operation_timeout
            .unwrap_or(DEFAULT_BROKER_OPERATION_TIMEOUT);
        let deadline = Instant::now() + budget;
        let request = CreateTopicsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(duration_to_ms(budget));
        // CreateTopics is routed to the controller.
        // Since Kafka 2.4 (KIP-590) any broker will forward admin requests to the
        // controller, but targeting the controller directly is still canonical in
        // the other clients.
        let response: CreateTopicsResponse = self
            .client
            .send(
                NodeTarget::Controller,
                ApiKey::CreateTopics,
                2,
                request,
                &opts,
            )
            .await?;

        if options.wait_for_propagation {
            // Only wait for topics the controller confirms are present on the
            // cluster log: code 0 (just created) or 36 (already existed). Any
            // other code means the topic was not created and never will be by
            // this call, so polling for it would burn the deadline pointlessly.
            let to_wait: Vec<TopicName> = response
                .topics
                .iter()
                .filter(|t| t.error_code == 0 || t.error_code == ERR_TOPIC_ALREADY_EXISTS)
                .map(|t| t.name.clone())
                .collect();
            if !to_wait.is_empty() {
                self.poll_topics_until(&to_wait, PropagationDirection::Visible, deadline)
                    .await?;
            }
        }

        Ok(response)
    }

    /// Delete topics by name. Routed to the controller.
    ///
    /// See [`Self::create_topics`] for the semantics of `options`.
    pub async fn delete_topics(
        &self,
        topic_names: Vec<TopicName>,
        options: DeleteTopicsOptions,
        opts: CallOptions,
    ) -> Result<DeleteTopicsResponse> {
        let budget = options
            .broker_operation_timeout
            .unwrap_or(DEFAULT_BROKER_OPERATION_TIMEOUT);
        let deadline = Instant::now() + budget;
        let request = DeleteTopicsRequest::default()
            .with_topic_names(topic_names)
            .with_timeout_ms(duration_to_ms(budget));
        let response: DeleteTopicsResponse = self
            .client
            .send(
                NodeTarget::Controller,
                ApiKey::DeleteTopics,
                4,
                request,
                &opts,
            )
            .await?;

        if options.wait_for_propagation {
            // Wait for topics the controller confirms are gone: code 0
            // (just deleted) or 3 (never existed / already gone — same
            // observable end state). Skip rows with other errors.
            let to_wait: Vec<TopicName> = response
                .responses
                .iter()
                .filter(|r| r.error_code == 0 || r.error_code == ERR_UNKNOWN_TOPIC_OR_PARTITION)
                .filter_map(|r| r.name.clone())
                .collect();
            if !to_wait.is_empty() {
                self.poll_topics_until(&to_wait, PropagationDirection::Gone, deadline)
                    .await?;
            }
        }

        Ok(response)
    }

    /// Increase the partition count of existing topics. Routed to the
    /// controller.
    ///
    /// Kafka does not allow shrinking the partition count. With
    /// `validate_only = true` the broker checks the request without applying
    /// it. Each response entry carries its own `error_code`.
    pub async fn create_partitions(
        &self,
        topics: Vec<CreatePartitionsTopic>,
        broker_operation_timeout: Option<Duration>,
        validate_only: bool,
        opts: CallOptions,
    ) -> Result<CreatePartitionsResponse> {
        let broker_operation_timeout =
            broker_operation_timeout.unwrap_or(DEFAULT_BROKER_OPERATION_TIMEOUT);
        let request = CreatePartitionsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(duration_to_ms(broker_operation_timeout))
            .with_validate_only(validate_only);
        self.client
            .send(
                NodeTarget::Controller,
                ApiKey::CreatePartitions,
                3,
                request,
                &opts,
            )
            .await
    }

    /// Return metadata for *all* topics in the cluster.
    pub async fn list_topics(&self, opts: CallOptions) -> Result<MetadataResponse> {
        // `topics: None` asks the broker for *all* topics.
        // `MetadataRequest::default()` yields `Some(vec![])`, which means *no* topics
        // (see CLAUDE.md), so we override explicitly.
        let request = MetadataRequest::default().with_topics(None);
        self.client
            .send(NodeTarget::AnyBroker, ApiKey::Metadata, 9, request, &opts)
            .await
    }

    /// Return metadata for the given topics.
    pub async fn describe_topics(
        &self,
        topics: Vec<MetadataRequestTopic>,
        opts: CallOptions,
    ) -> Result<MetadataResponse> {
        let request = MetadataRequest::default().with_topics(Some(topics));
        self.client
            .send(NodeTarget::AnyBroker, ApiKey::Metadata, 9, request, &opts)
            .await
    }

    /// Return broker list, controller id, and cluster id.
    pub async fn describe_cluster(
        &self,
        include_cluster_authorized_operations: bool,
        opts: CallOptions,
    ) -> Result<DescribeClusterResponse> {
        let request = DescribeClusterRequest::default()
            .with_include_cluster_authorized_operations(include_cluster_authorized_operations);
        self.client
            .send(
                NodeTarget::AnyBroker,
                ApiKey::DescribeCluster,
                0,
                request,
                &opts,
            )
            .await
    }

    /// Read configs for the given resources (topics, brokers, …).
    pub async fn describe_configs(
        &self,
        resources: Vec<DescribeConfigsResource>,
        opts: CallOptions,
    ) -> Result<DescribeConfigsResponse> {
        let request = DescribeConfigsRequest::default().with_resources(resources);
        self.client
            .send(
                NodeTarget::AnyBroker,
                ApiKey::DescribeConfigs,
                2,
                request,
                &opts,
            )
            .await
    }

    /// Modify configs incrementally (SET / DELETE / APPEND / SUBTRACT).
    pub async fn incremental_alter_configs(
        &self,
        resources: Vec<AlterConfigsResource>,
        validate_only: bool,
        opts: CallOptions,
    ) -> Result<IncrementalAlterConfigsResponse> {
        // Routed to the controller: correct for TOPIC resources, and for BROKER
        // resources KIP-590 controller forwarding handles it transparently on
        // Kafka >= 2.4.
        let request = IncrementalAlterConfigsRequest::default()
            .with_resources(resources)
            .with_validate_only(validate_only);
        self.client
            .send(
                NodeTarget::Controller,
                ApiKey::IncrementalAlterConfigs,
                1,
                request,
                &opts,
            )
            .await
    }

    /// Block until each topic is reported by every broker's MetadataCache
    /// with at least one partition that has an elected leader.
    ///
    /// Useful for callers that drive `create_topics` themselves (with
    /// `wait_for_propagation = false`) and need to assert on the raw
    /// `CreateTopicsResponse` before waiting.
    pub async fn wait_for_topics_visible(
        &self,
        topics: &[TopicName],
        timeout: Duration,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        self.poll_topics_until(
            topics,
            PropagationDirection::Visible,
            Instant::now() + timeout,
        )
        .await
    }

    /// Mirror of [`Self::wait_for_topics_visible`] for deletions: block
    /// until every broker reports each topic as
    /// `UNKNOWN_TOPIC_OR_PARTITION` (or omits it from the response).
    pub async fn wait_for_topics_gone(
        &self,
        topics: &[TopicName],
        timeout: Duration,
    ) -> Result<()> {
        if topics.is_empty() {
            return Ok(());
        }
        self.poll_topics_until(topics, PropagationDirection::Gone, Instant::now() + timeout)
            .await
    }

    /// Refresh metadata once to learn every broker id, then poll each
    /// broker's `MetadataRequest` view of `topics` until `accept(topic)`
    /// is true on every broker for every topic.
    ///
    /// In KRaft mode the controller commits the topic-creation /
    /// deletion record before the per-broker MetadataCache replays
    /// catch up. `MetadataRequest` against `AnyBroker` only proves the
    /// state on a single broker, but a Producer may target any other
    /// broker in the cluster — so we fan out one batched request per
    /// broker per round and require unanimous agreement before
    /// returning.
    ///
    /// On `Instant::now() >= deadline`, returns
    /// [`Error::TopicPropagationTimeout`] for the first topic still
    /// failing the predicate.
    async fn poll_topics_until(
        &self,
        topics: &[TopicName],
        direction: PropagationDirection,
        deadline: Instant,
    ) -> Result<()> {
        let accept: fn(&MetadataResponseTopic) -> bool = match direction {
            PropagationDirection::Visible => accept_visible,
            PropagationDirection::Gone => accept_gone,
        };

        let metadata = self.client.refresh_metadata().await?;
        let broker_ids: Vec<BrokerId> = metadata.brokers.iter().map(|b| b.node_id).collect();
        if broker_ids.is_empty() {
            return Err(Error::NoBrokerAvailable(
                "no brokers known to client — cannot poll for topic propagation".into(),
            ));
        }

        // Same shape as the test helper that this method replaces:
        // exponential backoff capped at 200ms keeps the loop responsive
        // when the replay catches up quickly without hammering the
        // brokers in pathological cases.
        let mut backoff = Duration::from_millis(20);
        let request_topics: Vec<MetadataRequestTopic> = topics
            .iter()
            .cloned()
            .map(|name| MetadataRequestTopic::default().with_name(Some(name)))
            .collect();

        loop {
            let mut still_failing: Option<TopicName> = None;
            'brokers: for &broker_id in &broker_ids {
                let request = MetadataRequest::default().with_topics(Some(request_topics.clone()));
                let resp: MetadataResponse = self
                    .client
                    .send(
                        NodeTarget::Broker(broker_id),
                        ApiKey::Metadata,
                        9,
                        request,
                        &CallOptions::default(),
                    )
                    .await?;

                for wanted in topics {
                    // `MetadataRequest` always echoes back an entry per
                    // requested topic, but a delete may be observed as a
                    // missing entry depending on broker version. Treat
                    // "missing" as `UNKNOWN_TOPIC_OR_PARTITION` so the
                    // gone-check is symmetric with the visible-check.
                    let synthetic_missing;
                    let topic = match resp.topics.iter().find(|t| t.name.as_ref() == Some(wanted)) {
                        Some(t) => t,
                        None => {
                            synthetic_missing = MetadataResponseTopic::default()
                                .with_error_code(ERR_UNKNOWN_TOPIC_OR_PARTITION);
                            &synthetic_missing
                        }
                    };
                    if !accept(topic) {
                        still_failing = Some(wanted.clone());
                        break 'brokers;
                    }
                }
            }

            match still_failing {
                None => return Ok(()),
                Some(topic) => {
                    if Instant::now() >= deadline {
                        return Err(Error::TopicPropagationTimeout { topic, direction });
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(200));
                }
            }
        }
    }
}

/// Predicate: a topic is "visible" when its top-level error is clear,
/// it has at least one partition, and every partition has a real
/// elected leader. KRaft replays the topic-creation record into
/// MetadataCache and ReplicaManager separately, so a broker can briefly
/// advertise the topic before any partition has an elected leader
/// (`leader_id == -1` / `LEADER_NOT_AVAILABLE` / `NOT_LEADER_OR_FOLLOWER`).
/// Sending a ProduceRequest in that window earns `NotLeaderOrFollower`,
/// which the producer doesn't yet retry per-partition — so we wait for
/// real leaders, not just for the topic to appear.
fn accept_visible(topic: &MetadataResponseTopic) -> bool {
    topic.error_code == 0
        && !topic.partitions.is_empty()
        && topic
            .partitions
            .iter()
            .all(|p| p.error_code == 0 && *p.leader_id >= 0)
}

/// Predicate: a topic is "gone" when the broker returns
/// `UNKNOWN_TOPIC_OR_PARTITION` for it (including the synthetic-missing
/// case `poll_topics_until` produces when the broker omits the entry).
fn accept_gone(topic: &MetadataResponseTopic) -> bool {
    topic.error_code == ERR_UNKNOWN_TOPIC_OR_PARTITION
}

// Mirrors the other clients.
const DEFAULT_BROKER_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

fn duration_to_ms(timeout: Duration) -> i32 {
    // Kafka admin RPCs carry timeouts as i32 milliseconds on the wire. Saturate at
    // i32::MAX (~24.8 days) rather than panic on absurd Durations.
    i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX)
}
