use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse};

use super::helpers;

async fn connect() -> crate::BrokerClient {
    let broker = helpers::plaintext_broker().await;
    let config = crate::Config::new(&broker.host, broker.port);
    let conn = crate::Connection::connect(&config, crate::Security::Plaintext)
        .await
        .unwrap();
    crate::BrokerClient::new(conn, crate::Auth::None)
        .await
        .unwrap()
}

fn metadata_request() -> (i16, MetadataRequest) {
    // `Metadata` v1 matches the version used by `BrokerClient::fetch_metadata`.
    let req = MetadataRequest::default().with_topics(None);
    (1, req)
}

#[tokio::test]
#[tracing_test::traced_test]
async fn send_request_drop_does_not_desync() {
    let client = connect().await;
    let (version, req) = metadata_request();

    // Queue a request, then drop the handle before the response arrives.
    // `read_task` must still pop the matching correlation id from the FIFO
    // without desynchronizing subsequent responses.
    let dropped = client
        .send_request::<_, MetadataResponse>(ApiKey::Metadata, version, &req)
        .await
        .unwrap();
    drop(dropped);

    // Follow-up send must succeed; if the VecDeque were desynced the
    // connection would tear down and this would return ConnectionAborted.
    let resp: MetadataResponse = client
        .send_request(ApiKey::Metadata, version, &req)
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(!resp.brokers.is_empty());
}

#[tokio::test]
#[tracing_test::traced_test]
async fn send_request_pipeline() {
    let client = connect().await;
    let (version, req) = metadata_request();

    // Queue many requests back-to-back before awaiting any response. All
    // responses share the same single connection and must decode cleanly
    // in FIFO order.
    const N: usize = 8;
    let mut handles = Vec::with_capacity(N);
    for _ in 0..N {
        let fut = client
            .send_request::<_, MetadataResponse>(ApiKey::Metadata, version, &req)
            .await
            .unwrap();
        handles.push(fut);
    }

    for fut in handles {
        let resp = fut.await.unwrap();
        assert!(!resp.brokers.is_empty());
    }
}
