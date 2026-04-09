use std::collections::HashMap;
use std::sync::Arc;

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::{BrokerId, CreateTopicsResponse, MetadataResponse};

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

pub struct Client {
    security: Security,
    auth: Auth,
    /// Captured from the bootstrap config that succeeded; reused when opening connections
    /// to brokers discovered via metadata.
    max_response_size: usize,
    controller_id: BrokerId,
    known_brokers: HashMap<BrokerId, BrokerInfo>,
    connections: HashMap<BrokerId, BrokerClient>,
    address_resolver: Option<AddressResolver>,
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

                    let mut known_brokers = HashMap::new();
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
                        known_brokers.insert(broker.node_id, BrokerInfo { host, port });
                    }

                    let mut connections = HashMap::new();
                    if let Some(node_id) = bootstrap_node_id {
                        connections.insert(node_id, client);
                    } else if let Some(&id) = known_brokers
                        .keys()
                        .find(|&&id| !connections.contains_key(&id))
                    {
                        connections.insert(id, client);
                    }

                    return Ok(Client {
                        security,
                        auth,
                        max_response_size: config.max_response_size,
                        controller_id: metadata.controller_id,
                        known_brokers,
                        connections,
                        address_resolver,
                    });
                }
                Err(e) => {
                    tracing::warn!(host = %config.host, port = config.port, error = %e, "bootstrap broker unreachable");
                    last_err = Some(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            Error::ProtocolError("no bootstrap brokers provided".into())
        }))
    }

    pub async fn controller(&mut self) -> Result<&BrokerClient> {
        self.broker(self.controller_id).await
    }

    pub async fn broker(&mut self, node_id: BrokerId) -> Result<&BrokerClient> {
        if !self.connections.contains_key(&node_id) {
            let info = self.known_brokers.get(&node_id).ok_or_else(|| {
                Error::ProtocolError(format!("unknown broker node_id: {}", node_id.0))
            })?;

            let config = Config::new(&info.host, info.port)
                .with_max_response_size(self.max_response_size);
            let conn = Connection::connect(&config, self.security.clone()).await?;
            let client = BrokerClient::new(conn, self.auth.clone()).await?;
            self.connections.insert(node_id, client);
        }

        Ok(&self.connections[&node_id])
    }

    pub fn any_broker(&self) -> Result<&BrokerClient> {
        self.connections
            .values()
            .next()
            .ok_or_else(|| Error::ProtocolError("no connected brokers".into()))
    }

    pub async fn refresh_metadata(&mut self) -> Result<MetadataResponse> {
        let broker = self.any_broker()?;
        let metadata = broker.fetch_metadata().await?;

        self.controller_id = metadata.controller_id;
        self.known_brokers.clear();
        for broker in &metadata.brokers {
            let (host, port) = resolve_address(
                &self.address_resolver,
                broker.node_id,
                broker.host.as_str(),
                broker.port,
            );
            self.known_brokers.insert(broker.node_id, BrokerInfo { host, port });
        }

        Ok(metadata)
    }

    pub async fn create_topics(
        &mut self,
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
        self.controller_id
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
