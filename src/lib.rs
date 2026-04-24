pub mod admin;
pub mod broker;
pub mod client;
pub mod config;
pub mod connection;
pub mod error;
pub mod secret;

pub use admin::AdminClient;
pub use broker::{Auth, BrokerClient, ReauthDelayFn, ResponseFuture, default_reauth_delay};
pub use client::{CallOptions, Client, ClientResponseFuture, NodeTarget};
pub use config::{Config, Security};
pub use connection::Connection;
pub use error::Error;
pub use secret::SecretString;

pub use kafka_protocol::messages::{BrokerId, TopicName};
pub use kafka_protocol::protocol::StrBytes;

#[cfg(test)]
mod tests;
