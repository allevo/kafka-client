use super::helpers;

#[tokio::test]
async fn test_standalone_sasl_plaintext_api_versions() {
    let broker = helpers::sasl_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let auth = crate::Auth::Plain {
        username: "admin".into(),
        password: crate::SecretString::new("admin-secret".into()),
    };
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext, auth)
        .await
        .unwrap();

    let versions = conn.api_versions();
    assert!(!versions.is_empty());
    assert!(versions.iter().any(|v| v.api_key == 17));
    assert!(versions.iter().any(|v| v.api_key == 36));
}

#[tokio::test]
async fn test_standalone_sasl_plaintext_bad_credentials() {
    let broker = helpers::sasl_broker().await;

    let config = crate::Config::new(&broker.host, broker.port);
    let auth = crate::Auth::Plain {
        username: "admin".into(),
        password: crate::SecretString::new("wrong-password".into()),
    };
    let result = crate::Connection::connect(&config, crate::Security::Plaintext, auth).await;
    assert!(matches!(result, Err(crate::Error::AuthenticationError(_))));
}
