use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::{
    ApiKey, CreateTopicsRequest, CreateTopicsResponse, MetadataRequest, MetadataResponse,
    RequestHeader, ResponseHeader,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

use crate::connection::Connection;
use crate::error::{Error, Result};

struct RequestMsg {
    correlation_id: i32,
    data: Vec<u8>,
    response_tx: oneshot::Sender<Result<Vec<u8>>>,
}

type InFlight = Arc<Mutex<HashMap<i32, oneshot::Sender<Result<Vec<u8>>>>>>;

#[derive(Clone)]
pub struct BrokerClient {
    request_tx: mpsc::Sender<RequestMsg>,
    next_correlation_id: Arc<AtomicI32>,
    api_versions: Arc<[ApiVersion]>,
}

const CLIENT_ID: &str = "kafka-client";

impl BrokerClient {
    pub fn new(connection: Connection) -> Self {
        let (stream, api_versions, last_correlation_id) = connection.into_parts();
        let (request_tx, request_rx) = mpsc::channel::<RequestMsg>(32);

        tracing::info!("spawning connection task");
        tokio::spawn(connection_task(stream, request_rx));

        BrokerClient {
            request_tx,
            next_correlation_id: Arc::new(AtomicI32::new(last_correlation_id + 1)),
            api_versions: api_versions.into(),
        }
    }

    pub async fn send_raw(
        &self,
        encode: impl FnOnce(i32) -> Vec<u8>,
    ) -> Result<Vec<u8>> {
        let correlation_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
        let data = encode(correlation_id);

        tracing::debug!(correlation_id, bytes = data.len(), "sending request");

        let (response_tx, response_rx) = oneshot::channel();

        let request = RequestMsg {
            correlation_id,
            data,
            response_tx,
        };

        self.request_tx.send(request).await.map_err(|_| {
            tracing::error!("connection task has shut down");
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "connection task has shut down",
            ))
        })?;

        let result = response_rx.await.map_err(|_| {
            tracing::error!(correlation_id, "connection task dropped without responding");
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "connection task dropped without responding",
            ))
        })?;

        match &result {
            Ok(buf) => tracing::debug!(correlation_id, bytes = buf.len(), "received response"),
            Err(e) => tracing::warn!(correlation_id, error = %e, "request failed"),
        }

        result
    }

    /// Send a typed Kafka request and decode the response.
    pub async fn send<Req, Resp>(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request: Req,
    ) -> Result<Resp>
    where
        Req: Encodable + HeaderVersion + Send + 'static,
        Resp: Decodable + HeaderVersion,
    {
        let response_data = self
            .send_raw(|correlation_id| {
                let header = RequestHeader::default()
                    .with_request_api_key(api_key as i16)
                    .with_request_api_version(api_version)
                    .with_correlation_id(correlation_id)
                    .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));

                let header_version = Req::header_version(api_version);
                let size = header.compute_size(header_version).unwrap()
                    + request.compute_size(api_version).unwrap();
                let mut buf = BytesMut::with_capacity(4 + size);
                buf.put_i32(size as i32);
                header.encode(&mut buf, header_version).unwrap();
                request.encode(&mut buf, api_version).unwrap();
                buf.to_vec()
            })
            .await?;

        let mut buf = Bytes::from(response_data);
        let resp_header_version = api_key.response_header_version(api_version);
        let _resp_header = ResponseHeader::decode(&mut buf, resp_header_version)?;
        let response = Resp::decode(&mut buf, api_version)?;
        Ok(response)
    }

    pub async fn fetch_metadata(&self) -> Result<MetadataResponse> {
        // topics: None requests metadata for all topics
        let request = MetadataRequest::default().with_topics(None);
        self.send(ApiKey::Metadata, 1, request).await
    }

    pub async fn create_topics(
        &self,
        topics: Vec<CreatableTopic>,
        timeout_ms: i32,
    ) -> Result<CreateTopicsResponse> {
        let request = CreateTopicsRequest::default()
            .with_topics(topics)
            .with_timeout_ms(timeout_ms);
        self.send(ApiKey::CreateTopics, 2, request).await
    }

    pub fn api_versions(&self) -> &[ApiVersion] {
        &self.api_versions
    }
}

async fn connection_task(
    stream: crate::connection::Stream,
    mut request_rx: mpsc::Receiver<RequestMsg>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let in_flight: InFlight = Arc::new(Mutex::new(HashMap::new()));

    let in_flight_w = Arc::clone(&in_flight);
    let write_loop = async move {
        while let Some(req) = request_rx.recv().await {
            tracing::trace!(correlation_id = req.correlation_id, bytes = req.data.len(), "writing request to broker");
            {
                let mut map = in_flight_w.lock().unwrap();
                map.insert(req.correlation_id, req.response_tx);
            }
            if let Err(e) = writer.write_all(&req.data).await {
                tracing::error!(correlation_id = req.correlation_id, error = %e, "write failed");
                let mut map = in_flight_w.lock().unwrap();
                if let Some(tx) = map.remove(&req.correlation_id) {
                    let _ = tx.send(Err(Error::Io(std::io::Error::new(e.kind(), e.to_string()))));
                }
                return;
            }
            if writer.flush().await.is_err() {
                tracing::error!("flush failed, closing write loop");
                return;
            }
        }
        tracing::debug!("request channel closed, write loop exiting");
    };

    let in_flight_r = Arc::clone(&in_flight);
    let read_loop = async move {
        let mut size_buf = [0u8; 4];
        loop {
            if reader.read_exact(&mut size_buf).await.is_err() {
                tracing::warn!("connection lost while reading response size");
                drain_in_flight(&in_flight_r, "connection lost");
                return;
            }

            let response_size = i32::from_be_bytes(size_buf);
            if response_size <= 0 || response_size > 10 * 1024 * 1024 {
                tracing::error!(response_size, "invalid response size, closing connection");
                drain_in_flight(
                    &in_flight_r,
                    &format!("invalid response size: {response_size}"),
                );
                return;
            }

            let mut response_buf = vec![0u8; response_size as usize];
            if reader.read_exact(&mut response_buf).await.is_err() {
                tracing::warn!("connection lost while reading response body");
                drain_in_flight(&in_flight_r, "connection lost");
                return;
            }

            if response_buf.len() < 4 {
                tracing::warn!(response_size, "response too short, skipping");
                continue;
            }
            let corr_id = i32::from_be_bytes(response_buf[0..4].try_into().unwrap());
            tracing::trace!(correlation_id = corr_id, bytes = response_buf.len(), "received response from broker");

            let tx = {
                let mut map = in_flight_r.lock().unwrap();
                map.remove(&corr_id)
            };
            if let Some(tx) = tx {
                let _ = tx.send(Ok(response_buf));
            } else {
                tracing::warn!(correlation_id = corr_id, "no in-flight request for correlation id");
            }
        }
    };

    tokio::select! {
        _ = write_loop => {}
        _ = read_loop => {}
    }

    tracing::info!("connection task exiting");
    drain_in_flight(&in_flight, "connection task exited");
}

fn drain_in_flight(in_flight: &InFlight, message: &str) {
    let mut map = in_flight.lock().unwrap();
    let count = map.len();
    if count > 0 {
        tracing::warn!(count, reason = message, "draining in-flight requests");
    }
    for (_, tx) in map.drain() {
        let _ = tx.send(Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            message.to_string(),
        ))));
    }
}
