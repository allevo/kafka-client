use std::collections::HashMap;
use std::sync::LazyLock;

use testcontainers::runners::AsyncRunner;
use testcontainers::ImageExt;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use tokio::sync::OnceCell;

use super::containers;

pub struct SharedBroker {
    pub host: String,
    pub port: u16,
    // Type-erased container handle — kept alive so Docker doesn't stop the broker
    _container: Box<dyn std::any::Any + Send + Sync>,
}

pub struct SharedCluster {
    pub addr_map: HashMap<i32, (String, u16)>,
    _containers: Vec<Box<dyn std::any::Any + Send + Sync>>,
}

static PLAINTEXT: LazyLock<OnceCell<SharedBroker>> = LazyLock::new(OnceCell::new);
static TLS: LazyLock<OnceCell<SharedBroker>> = LazyLock::new(OnceCell::new);
static SASL: LazyLock<OnceCell<SharedBroker>> = LazyLock::new(OnceCell::new);
static CLUSTER: LazyLock<OnceCell<SharedCluster>> = LazyLock::new(OnceCell::new);

pub async fn plaintext_broker() -> &'static SharedBroker {
    PLAINTEXT
        .get_or_init(|| async {
            let kafka = Kafka::default().start().await.unwrap();
            let host = kafka.get_host().await.unwrap().to_string();
            let port = kafka.get_host_port_ipv4(KAFKA_PORT).await.unwrap();
            SharedBroker {
                host,
                port,
                _container: Box::new(kafka),
            }
        })
        .await
}

pub async fn tls_broker() -> &'static SharedBroker {
    TLS.get_or_init(|| async {
        let kafka = containers::standalone_tls_broker()
            .start()
            .await
            .unwrap();
        let host = kafka.get_host().await.unwrap().to_string();
        let port = kafka
            .get_host_port_ipv4(containers::SSL_PORT)
            .await
            .unwrap();
        SharedBroker {
            host,
            port,
            _container: Box::new(kafka),
        }
    })
    .await
}

pub async fn sasl_broker() -> &'static SharedBroker {
    SASL.get_or_init(|| async {
        let kafka = containers::standalone_sasl_plaintext_broker()
            .start()
            .await
            .unwrap();
        let host = kafka.get_host().await.unwrap().to_string();
        let port = kafka
            .get_host_port_ipv4(containers::SASL_PLAINTEXT_PORT)
            .await
            .unwrap();
        SharedBroker {
            host,
            port,
            _container: Box::new(kafka),
        }
    })
    .await
}

pub async fn plaintext_cluster() -> &'static SharedCluster {
    CLUSTER
        .get_or_init(|| async {
            let prefix = "shared";
            let network = &format!("{prefix}-network");
            let quorum_voters = containers::quorum_voters_plaintext(prefix);

            let (n1, n2, n3) = tokio::try_join!(
                containers::kraft_broker_plaintext(prefix, 1, &quorum_voters)
                    .with_network(network)
                    .start(),
                containers::kraft_broker_plaintext(prefix, 2, &quorum_voters)
                    .with_network(network)
                    .start(),
                containers::kraft_broker_plaintext(prefix, 3, &quorum_voters)
                    .with_network(network)
                    .start(),
            )
            .unwrap();

            let mut addr_map = HashMap::new();
            for (node_id, node) in [(1, &n1), (2, &n2), (3, &n3)] {
                let host = node.get_host().await.unwrap().to_string();
                let port = node
                    .get_host_port_ipv4(containers::KAFKA_PORT)
                    .await
                    .unwrap();
                addr_map.insert(node_id, (host, port));
            }

            SharedCluster {
                addr_map,
                _containers: vec![Box::new(n1), Box::new(n2), Box::new(n3)],
            }
        })
        .await
}
