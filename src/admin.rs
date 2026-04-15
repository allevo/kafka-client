use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::{ApiKey, CreateTopicsRequest, CreateTopicsResponse};

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

    pub async fn create_topics(
        &self,
        topics: Vec<CreatableTopic>,
        timeout_ms: i32,
    ) -> Result<CreateTopicsResponse> {
        // CreateTopics is routed to the controller.
        // Since Kafka 2.4 (KIP-590) any broker will forward admin requests to the
        // controller, but targeting the controller directly is still canonical in
        // the other clients.
        self.client
            .send(
                NodeTarget::Controller,
                ApiKey::CreateTopics,
                2,
                move |_| {
                    CreateTopicsRequest::default()
                        .with_topics(topics.clone())
                        .with_timeout_ms(timeout_ms)
                },
            )
            .await
    }
}
