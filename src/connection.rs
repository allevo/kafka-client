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

pub enum Security {
    Plaintext,
    Ssl(Arc<rustls::ClientConfig>),
}

enum Stream {
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
}

impl Connection {
    pub async fn connect(config: &Config, security: Security) -> Result<Self> {
        let addr = format!("{}:{}", config.host, config.port);
        let tcp = TcpStream::connect(&addr).await?;

        let mut stream = match security {
            Security::Plaintext => Stream::Plain(tcp),
            Security::Ssl(tls_config) => {
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
                Stream::Tls(tls_stream)
            }
        };

        let correlation_id = 1;

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

        Ok(Connection {
            stream,
            api_versions: versions,
        })
    }

    pub fn api_versions(&self) -> &[ApiVersion] {
        &self.api_versions
    }
}
