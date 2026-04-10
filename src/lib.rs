pub mod broker;
pub mod client;
pub mod config;
pub mod connection;
pub mod error;
pub mod secret;

pub use broker::{Auth, BrokerClient};
pub use client::Client;
pub use config::{Config, Security};
pub use connection::Connection;
pub use error::Error;
pub use secret::SecretString;

pub use kafka_protocol::messages::{BrokerId, TopicName};
pub use kafka_protocol::protocol::StrBytes;

#[cfg(test)]
mod tests;
