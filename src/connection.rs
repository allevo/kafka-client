use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::protocol::ApiVersion;
use crate::protocol::api_versions;
use crate::protocol::sasl_authenticate;
use crate::protocol::sasl_handshake;
use crate::secret::SecretString;

pub enum Security {
    Plaintext,
    Ssl(Arc<rustls::ClientConfig>),
}

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

        correlation_id += 1;
        tracing::debug!(correlation_id, "sending ApiVersions request");
        let request = api_versions::encode_request_v0(correlation_id, "kafka-client");
        stream.write_all(&request).await?;

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

        let (response_correlation_id, versions) = api_versions::decode_response_v0(&response_buf)?;
        if response_correlation_id != correlation_id {
            return Err(Error::ProtocolError(format!(
                "correlation id mismatch: expected 1, got {response_correlation_id}"
            )));
        }
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

            // SaslHandshake
            correlation_id += 1;
            tracing::debug!(correlation_id, "sending SaslHandshake request");
            let request =
                sasl_handshake::encode_request_v1(correlation_id, "kafka-client", "PLAIN");
            stream.write_all(&request).await?;

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
            let (resp_corr_id, _mechanisms) =
                sasl_handshake::decode_response_v1(&response_buf)?;
            if resp_corr_id != correlation_id {
                return Err(Error::ProtocolError(format!(
                    "correlation id mismatch: expected {correlation_id}, got {resp_corr_id}"
                )));
            }
            tracing::debug!(correlation_id, "SaslHandshake complete");

            // SaslAuthenticate
            correlation_id += 1;
            tracing::debug!(correlation_id, "sending SaslAuthenticate request");
            let password = password.expose_secret();
            let mut token = zeroize::Zeroizing::new(
                Vec::with_capacity(1 + username.len() + 1 + password.len()),
            );
            token.push(0u8);
            token.extend_from_slice(username.as_bytes());
            token.push(0u8);
            token.extend_from_slice(password.as_bytes());

            let request =
                sasl_authenticate::encode_request_v0(correlation_id, "kafka-client", &token);
            stream.write_all(&request).await?;

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
            let resp_corr_id = sasl_authenticate::decode_response_v0(&response_buf)?;
            if resp_corr_id != correlation_id {
                return Err(Error::ProtocolError(format!(
                    "correlation id mismatch: expected {correlation_id}, got {resp_corr_id}"
                )));
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
