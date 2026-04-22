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
    /// `timeout` is the broker-side wait for partition assignment to converge;
    /// it is independent of the client's own request timeout.
    pub async fn create_topics(
        &self,
        topics: Vec<CreatableTopic>,
        timeout: Option<Duration>,
    ) -> Result<CreateTopicsResponse> {
        // Match other client implementations.
        let timeout = timeout.unwrap_or(DEFAULT_API_TIMEOUT);
        let request = CreateTopicsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(duration_to_ms(timeout));
        // CreateTopics is routed to the controller.
        // Since Kafka 2.4 (KIP-590) any broker will forward admin requests to the
        // controller, but targeting the controller directly is still canonical in
        // the other clients.
        self.client
            .send(NodeTarget::Controller, ApiKey::CreateTopics, 2, request)
            .await
    }

    /// Delete topics by name. Routed to the controller.
    pub async fn delete_topics(
        &self,
        topic_names: Vec<TopicName>,
        timeout: Duration,
    ) -> Result<DeleteTopicsResponse> {
        let request = DeleteTopicsRequest::default()
            .with_topic_names(topic_names)
            .with_timeout_ms(duration_to_ms(timeout));
        self.client
            .send(NodeTarget::Controller, ApiKey::DeleteTopics, 4, request)
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
        timeout: Duration,
        validate_only: bool,
    ) -> Result<CreatePartitionsResponse> {
        let request = CreatePartitionsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(duration_to_ms(timeout))
            .with_validate_only(validate_only);
        self.client
            .send(NodeTarget::Controller, ApiKey::CreatePartitions, 3, request)
            .await
    }

    /// Return metadata for *all* topics in the cluster.
    pub async fn list_topics(&self) -> Result<MetadataResponse> {
        // `topics: None` asks the broker for *all* topics.
        // `MetadataRequest::default()` yields `Some(vec![])`, which means *no* topics
        // (see CLAUDE.md), so we override explicitly.
        let request = MetadataRequest::default().with_topics(None);
        self.client
            .send(NodeTarget::AnyBroker, ApiKey::Metadata, 9, request)
            .await
    }

    /// Return metadata for the given topics.
    pub async fn describe_topics(
        &self,
        topics: Vec<MetadataRequestTopic>,
    ) -> Result<MetadataResponse> {
        let request = MetadataRequest::default().with_topics(Some(topics));
        self.client
            .send(NodeTarget::AnyBroker, ApiKey::Metadata, 9, request)
            .await
    }

    /// Return broker list, controller id, and cluster id.
    pub async fn describe_cluster(
        &self,
        include_cluster_authorized_operations: bool,
    ) -> Result<DescribeClusterResponse> {
        let request = DescribeClusterRequest::default()
            .with_include_cluster_authorized_operations(include_cluster_authorized_operations);
        self.client
            .send(NodeTarget::AnyBroker, ApiKey::DescribeCluster, 0, request)
            .await
    }

    /// Read configs for the given resources (topics, brokers, …).
    pub async fn describe_configs(
        &self,
        resources: Vec<DescribeConfigsResource>,
    ) -> Result<DescribeConfigsResponse> {
        let request = DescribeConfigsRequest::default().with_resources(resources);
        self.client
            .send(NodeTarget::AnyBroker, ApiKey::DescribeConfigs, 2, request)
            .await
    }

    /// Modify configs incrementally (SET / DELETE / APPEND / SUBTRACT).
    pub async fn incremental_alter_configs(
        &self,
        resources: Vec<AlterConfigsResource>,
        validate_only: bool,
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
            )
            .await
    }
}

// Mirrors the other clients.
const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(60);

fn duration_to_ms(timeout: Duration) -> i32 {
    // Kafka admin RPCs carry timeouts as i32 milliseconds on the wire. Saturate at
    // i32::MAX (~24.8 days) rather than panic on absurd Durations.
    i32::try_from(timeout.as_millis()).unwrap_or(i32::MAX)
}
