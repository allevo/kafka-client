use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{
    ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, HeaderVersion, StrBytes};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::secret::SecretString;

#[derive(Clone)]
pub enum Security {
    Plaintext,
    Ssl(Arc<rustls::ClientConfig>),
}

#[derive(Clone)]
pub enum Auth {
    None,
    Plain { username: String, password: SecretString },
}

pub(crate) enum Stream {
    Plain(TcpStream),
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Stream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_flush(cx),
            Stream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Stream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

pub struct Connection {
    stream: Stream,
    api_versions: Vec<ApiVersion>,
    correlation_id: i32,
}

const CLIENT_ID: &str = "kafka-client";

/// Encode a Kafka request with its header and 4-byte size prefix.
fn encode_request<R: Encodable + HeaderVersion>(
    request: &R,
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
) -> Result<BytesMut> {
    let header = RequestHeader::default()
        .with_request_api_key(api_key)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_static_str(CLIENT_ID)));

    let header_version = R::header_version(api_version);
    let size = header.compute_size(header_version)? + request.compute_size(api_version)?;

    let mut buf = BytesMut::with_capacity(4 + size);
    buf.put_i32(size as i32);
    header.encode(&mut buf, header_version)?;
    request.encode(&mut buf, api_version)?;
    Ok(buf)
}

/// Read a framed response from the stream: 4-byte size prefix, then payload.
async fn read_response(stream: &mut Stream) -> Result<Bytes> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let response_size = i32::from_be_bytes(size_buf);

    if response_size <= 0 || response_size > 1024 * 1024 {
        return Err(Error::ProtocolError(format!(
            "invalid response size: {response_size}"
        )));
    }

    let mut response_buf = vec![0u8; response_size as usize];
    stream.read_exact(&mut response_buf).await?;
    Ok(Bytes::from(response_buf))
}

/// Decode a response header and verify correlation ID.
fn decode_response_header(
    buf: &mut Bytes,
    header_version: i16,
    expected_correlation_id: i32,
) -> Result<()> {
    let resp_header = ResponseHeader::decode(buf, header_version)?;
    if resp_header.correlation_id != expected_correlation_id {
        return Err(Error::ProtocolError(format!(
            "correlation id mismatch: expected {expected_correlation_id}, got {}",
            resp_header.correlation_id
        )));
    }
    Ok(())
}

impl Connection {
    pub async fn connect(config: &Config, security: Security, auth: Auth) -> Result<Self> {
        let addr = format!("{}:{}", config.host, config.port);
        tracing::info!(addr = %addr, "connecting to broker");
        let tcp = TcpStream::connect(&addr).await?;
        tracing::debug!(addr = %addr, "TCP connection established");

        let mut stream = match security {
            Security::Plaintext => {
                tracing::debug!("using plaintext connection");
                Stream::Plain(tcp)
            }
            Security::Ssl(tls_config) => {
                tracing::debug!(host = %config.host, "starting TLS handshake");
                let server_name = rustls::pki_types::ServerName::try_from(config.host.as_str())
                    .map_err(|_| {
                        Error::ProtocolError(format!(
                            "invalid TLS server name: {}",
                            config.host
                        ))
                    })?
                    .to_owned();
                let connector = TlsConnector::from(tls_config);
                let tls_stream = connector.connect(server_name, tcp).await?;
                tracing::debug!(host = %config.host, "TLS handshake complete");
                Stream::Tls(tls_stream)
            }
        };

        let mut correlation_id: i32 = 0;

        // ApiVersions request (v0)
        let api_version: i16 = 0;
        correlation_id += 1;
        tracing::debug!(correlation_id, "sending ApiVersions request");
        let request_buf = encode_request(
            &ApiVersionsRequest::default(),
            18, // ApiVersions api_key
            api_version,
            correlation_id,
        )?;
        stream.write_all(&request_buf).await?;

        let mut response_bytes = read_response(&mut stream).await?;
        let resp_header_version = ApiVersionsResponse::header_version(api_version);
        decode_response_header(&mut response_bytes, resp_header_version, correlation_id)?;
        let versions_response = ApiVersionsResponse::decode(&mut response_bytes, api_version)?;

        if versions_response.error_code != 0 {
            return Err(Error::ApiError {
                error_code: versions_response.error_code,
            });
        }

        let versions = versions_response.api_keys;
        tracing::debug!(api_count = versions.len(), "received ApiVersions response");

        if let Auth::Plain {
            username, password, ..
        } = &auth
        {
            tracing::info!(username = %username, "starting SASL/PLAIN authentication");

            let has_sasl_handshake = versions.iter().any(|v| v.api_key == 17);
            let has_sasl_authenticate = versions.iter().any(|v| v.api_key == 36);
            if !has_sasl_handshake || !has_sasl_authenticate {
                return Err(Error::AuthenticationError(
                    "broker does not support SASL authentication (missing API key 17 or 36)"
                        .into(),
                ));
            }

            // SaslHandshake (v1)
            let api_version: i16 = 1;
            correlation_id += 1;
            tracing::debug!(correlation_id, "sending SaslHandshake request");
            let request_buf = encode_request(
                &SaslHandshakeRequest::default()
                    .with_mechanism(StrBytes::from_static_str("PLAIN")),
                17, // SaslHandshake api_key
                api_version,
                correlation_id,
            )?;
            stream.write_all(&request_buf).await?;

            let mut response_bytes = read_response(&mut stream).await?;
            let resp_header_version = SaslHandshakeResponse::header_version(api_version);
            decode_response_header(&mut response_bytes, resp_header_version, correlation_id)?;
            let handshake_response =
                SaslHandshakeResponse::decode(&mut response_bytes, api_version)?;

            if handshake_response.error_code != 0 {
                return Err(Error::AuthenticationError(format!(
                    "SASL handshake failed with error code: {}",
                    handshake_response.error_code
                )));
            }
            tracing::debug!(correlation_id, "SaslHandshake complete");

            // SaslAuthenticate (v0)
            let api_version: i16 = 0;
            correlation_id += 1;
            tracing::debug!(correlation_id, "sending SaslAuthenticate request");
            let password = password.expose_secret();
            let mut token = Vec::with_capacity(1 + username.len() + 1 + password.len());
            token.push(0u8);
            token.extend_from_slice(username.as_bytes());
            token.push(0u8);
            token.extend_from_slice(password.as_bytes());

            let request_buf = encode_request(
                &SaslAuthenticateRequest::default()
                    .with_auth_bytes(Bytes::copy_from_slice(&token)),
                36, // SaslAuthenticate api_key
                api_version,
                correlation_id,
            )?;
            stream.write_all(&request_buf).await?;

            let mut response_bytes = read_response(&mut stream).await?;
            let resp_header_version = SaslAuthenticateResponse::header_version(api_version);
            decode_response_header(&mut response_bytes, resp_header_version, correlation_id)?;
            let auth_response =
                SaslAuthenticateResponse::decode(&mut response_bytes, api_version)?;

            if auth_response.error_code != 0 {
                let msg = auth_response
                    .error_message
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("error code: {}", auth_response.error_code));
                return Err(Error::AuthenticationError(msg));
            }
            tracing::info!(username = %username, "SASL/PLAIN authentication successful");
        }

        tracing::info!(addr = %addr, "connected to broker");
        Ok(Connection {
            stream,
            api_versions: versions,
            correlation_id,
        })
    }

    pub fn api_versions(&self) -> &[ApiVersion] {
        &self.api_versions
    }

    pub(crate) fn into_parts(self) -> (Stream, Vec<ApiVersion>, i32) {
        (self.stream, self.api_versions, self.correlation_id)
    }
}
