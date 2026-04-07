mod common;

use testcontainers::runners::AsyncRunner;

#[tokio::test]
async fn test_standalone_sasl_plaintext_api_versions() {
    let kafka = common::standalone_sasl_plaintext_broker()
        .start()
        .await
        .unwrap();

    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka
        .get_host_port_ipv4(common::SASL_PLAINTEXT_PORT)
        .await
        .unwrap();

    let config = kafka_client::Config::new(&host, port);
    let auth = kafka_client::Auth::Plain {
        username: "admin".into(),
        password: "admin-secret".into(),
    };
    let conn =
        kafka_client::Connection::connect(&config, kafka_client::Security::Plaintext, auth)
            .await
            .unwrap();

    let versions = conn.api_versions();
    assert!(!versions.is_empty());
    assert!(versions.iter().any(|v| v.api_key == 17));
    assert!(versions.iter().any(|v| v.api_key == 36));
}

#[tokio::test]
async fn test_standalone_sasl_plaintext_bad_credentials() {
    let kafka = common::standalone_sasl_plaintext_broker()
        .start()
        .await
        .unwrap();

    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka
        .get_host_port_ipv4(common::SASL_PLAINTEXT_PORT)
        .await
        .unwrap();

    let config = kafka_client::Config::new(&host, port);
    let auth = kafka_client::Auth::Plain {
        username: "admin".into(),
        password: "wrong-password".into(),
    };
    let result =
        kafka_client::Connection::connect(&config, kafka_client::Security::Plaintext, auth).await;
    assert!(matches!(
        result,
        Err(kafka_client::Error::AuthenticationError(_))
    ));
}
