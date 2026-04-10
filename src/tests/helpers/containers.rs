use std::path::PathBuf;
use std::time::Duration;

use testcontainers::core::IntoContainerPort;
use testcontainers::core::WaitFor;
use testcontainers::{ContainerRequest, GenericImage, ImageExt};

pub const IMAGE: &str = "apache/kafka";
pub const TAG: &str = "3.7.0";
pub const CLUSTER_ID: &str = "4L6g3nShT-eMCtK--X86sw";
pub const KAFKA_PORT: u16 = 9092;
pub const SSL_PORT: u16 = 9093;
pub const SASL_PLAINTEXT_PORT: u16 = 9094;

pub fn fixtures_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("secrets")
}

fn read_fixture(name: &str) -> Vec<u8> {
    std::fs::read(fixtures_path().join(name))
        .unwrap_or_else(|_| panic!("Failed to read fixture: {name}"))
}

fn with_ssl_files(req: ContainerRequest<GenericImage>) -> ContainerRequest<GenericImage> {
    let files = [
        "kafka01.keystore.jks",
        "kafka.truststore.jks",
        "kafka_keystore_creds",
        "kafka_ssl_key_creds",
        "kafka_truststore_creds",
    ];
    let mut req = req;
    for name in files {
        req = req.with_copy_to(format!("/etc/kafka/secrets/{name}"), read_fixture(name));
    }
    req
}

pub fn quorum_voters_plaintext(prefix: &str) -> String {
    format!("1@{prefix}-kafka-1:9093,2@{prefix}-kafka-2:9093,3@{prefix}-kafka-3:9093")
}

pub fn kraft_broker_plaintext(
    prefix: &str,
    node_id: u8,
    quorum_voters: &str,
) -> ContainerRequest<GenericImage> {
    let name = format!("{prefix}-kafka-{node_id}");
    GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_exposed_port(KAFKA_PORT.tcp())
        .with_container_name(&name)
        .with_env_var("KAFKA_NODE_ID", node_id.to_string())
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", quorum_voters)
        .with_env_var(
            "KAFKA_LISTENERS",
            format!("PLAINTEXT://:19092,CONTROLLER://:9093,PLAINTEXT_HOST://:{KAFKA_PORT}"),
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://{name}:19092,PLAINTEXT_HOST://localhost:{KAFKA_PORT}"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("CLUSTER_ID", CLUSTER_ID)
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs")
        .with_startup_timeout(Duration::from_secs(120))
}

pub fn standalone_tls_broker() -> ContainerRequest<GenericImage> {
    let req = GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_exposed_port(SSL_PORT.tcp())
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "SSL:SSL,CONTROLLER:PLAINTEXT,SSL-INTERNAL:SSL",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!("SSL://:{SSL_PORT},CONTROLLER://:29093,SSL-INTERNAL://:19093"),
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "SSL-INTERNAL")
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("SSL://localhost:{SSL_PORT},SSL-INTERNAL://localhost:19093"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("CLUSTER_ID", CLUSTER_ID)
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs")
        .with_env_var("KAFKA_SSL_KEYSTORE_FILENAME", "kafka01.keystore.jks")
        .with_env_var("KAFKA_SSL_KEYSTORE_CREDENTIALS", "kafka_keystore_creds")
        .with_env_var("KAFKA_SSL_KEY_CREDENTIALS", "kafka_ssl_key_creds")
        .with_env_var("KAFKA_SSL_TRUSTSTORE_FILENAME", "kafka.truststore.jks")
        .with_env_var("KAFKA_SSL_TRUSTSTORE_CREDENTIALS", "kafka_truststore_creds")
        .with_env_var("KAFKA_SSL_CLIENT_AUTH", "required")
        .with_env_var("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "")
        .with_startup_timeout(Duration::from_secs(120));

    with_ssl_files(req)
}

pub fn standalone_sasl_plaintext_broker() -> ContainerRequest<GenericImage> {
    let jaas_config = b"KafkaServer {\n\
        org.apache.kafka.common.security.plain.PlainLoginModule required\n\
        username=\"admin\"\n\
        password=\"admin-secret\"\n\
        user_admin=\"admin-secret\"\n\
        user_alice=\"alice-secret\";\n\
    };\n";

    GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_exposed_port(SASL_PLAINTEXT_PORT.tcp())
        .with_copy_to("/etc/kafka/secrets/jaas.conf", jaas_config.to_vec())
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://:19092,SASL_PLAINTEXT://:{SASL_PLAINTEXT_PORT},CONTROLLER://:29093"
            ),
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://localhost:19092,SASL_PLAINTEXT://localhost:{SASL_PLAINTEXT_PORT}"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("CLUSTER_ID", CLUSTER_ID)
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs")
        .with_env_var("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .with_env_var(
            "KAFKA_OPTS",
            "-Djava.security.auth.login.config=/etc/kafka/secrets/jaas.conf",
        )
        .with_startup_timeout(Duration::from_secs(120))
}

/// SASL broker with a short `connections.max.reauth.ms` to exercise re-authentication.
pub fn standalone_sasl_reauth_broker(reauth_ms: u32) -> ContainerRequest<GenericImage> {
    let jaas_config = b"KafkaServer {\n\
        org.apache.kafka.common.security.plain.PlainLoginModule required\n\
        username=\"admin\"\n\
        password=\"admin-secret\"\n\
        user_admin=\"admin-secret\"\n\
        user_alice=\"alice-secret\";\n\
    };\n";

    GenericImage::new(IMAGE, TAG)
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_exposed_port(SASL_PLAINTEXT_PORT.tcp())
        .with_copy_to("/etc/kafka/secrets/jaas.conf", jaas_config.to_vec())
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT",
        )
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:29093")
        .with_env_var(
            "KAFKA_LISTENERS",
            format!(
                "PLAINTEXT://:19092,SASL_PLAINTEXT://:{SASL_PLAINTEXT_PORT},CONTROLLER://:29093"
            ),
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://localhost:19092,SASL_PLAINTEXT://localhost:{SASL_PLAINTEXT_PORT}"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var("CLUSTER_ID", CLUSTER_ID)
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env_var("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env_var("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs")
        .with_env_var("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
        .with_env_var("KAFKA_CONNECTIONS_MAX_REAUTH_MS", reauth_ms.to_string())
        .with_env_var(
            "KAFKA_OPTS",
            "-Djava.security.auth.login.config=/etc/kafka/secrets/jaas.conf",
        )
        .with_startup_timeout(Duration::from_secs(120))
}

pub async fn assert_tcp_reachable(host: &str, port: u16) {
    let addr = format!("{host}:{port}");
    tokio::net::TcpStream::connect(&addr)
        .await
        .unwrap_or_else(|e| panic!("Failed to connect to Kafka broker at {addr}: {e}"));
}
