use std::time::Duration;

use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    ApiKey, CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest,
    CreateTopicsResponse, DeleteTopicsRequest, DeleteTopicsResponse, DescribeClusterRequest,
    DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, MetadataRequest,
    MetadataResponse, TopicName,
};

use crate::client::{Client, NodeTarget};
use crate::error::Result;

/// Per-call overrides for the client-side retry loop that governs every
/// admin RPC. falls back to [`crate::Config`].
#[derive(Debug, Default, Clone)]
pub struct AdminOptions {
    timeout: Option<Duration>,
    retries: Option<u32>,
    retry_backoff: Option<Duration>,
    retry_backoff_max: Option<Duration>,
}

impl AdminOptions {
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
    /// `broker_operation_timeout` is the broker-side wait for partition
    /// assignment to converge (wire `timeout_ms`); it is independent of the
    /// client-side deadline carried by `opts`.
    pub async fn create_topics(
        &self,
        topics: Vec<CreatableTopic>,
        broker_operation_timeout: Option<Duration>,
        opts: AdminOptions,
    ) -> Result<CreateTopicsResponse> {
        // Match other client implementations.
        let broker_operation_timeout =
            broker_operation_timeout.unwrap_or(DEFAULT_BROKER_OPERATION_TIMEOUT);
        let request = CreateTopicsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(duration_to_ms(broker_operation_timeout));
        // CreateTopics is routed to the controller.
        // Since Kafka 2.4 (KIP-590) any broker will forward admin requests to the
        // controller, but targeting the controller directly is still canonical in
        // the other clients.
        self.client
            .send(
                NodeTarget::Controller,
                ApiKey::CreateTopics,
                2,
                request,
                &opts,
            )
            .await
    }

    /// Delete topics by name. Routed to the controller.
    pub async fn delete_topics(
        &self,
        topic_names: Vec<TopicName>,
        broker_operation_timeout: Option<Duration>,
        opts: AdminOptions,
    ) -> Result<DeleteTopicsResponse> {
        let broker_operation_timeout =
            broker_operation_timeout.unwrap_or(DEFAULT_BROKER_OPERATION_TIMEOUT);
        let request = DeleteTopicsRequest::default()
            .with_topic_names(topic_names)
            .with_timeout_ms(duration_to_ms(broker_operation_timeout));
        self.client
            .send(
                NodeTarget::Controller,
                ApiKey::DeleteTopics,
                4,
                request,
                &opts,
            )
            .await
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
        opts: AdminOptions,
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
    pub async fn list_topics(&self, opts: AdminOptions) -> Result<MetadataResponse> {
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
        opts: AdminOptions,
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
        opts: AdminOptions,
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
        opts: AdminOptions,
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
        opts: AdminOptions,
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
}

// Mirrors the other clients.
const DEFAULT_BROKER_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

fn duration_to_ms(timeout: Duration) -> i32 {
    // Kafka admin RPCs carry timeouts as i32 milliseconds on the wire. Saturate at
    // i32::MAX (~24.8 days) rather than panic on absurd Durations.
    i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX)
}
