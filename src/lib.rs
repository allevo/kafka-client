pub mod client;
pub mod cluster;
pub mod config;
pub mod connection;
pub mod error;
pub mod secret;

pub use client::BrokerClient;
pub use cluster::Client;
pub use config::Config;
pub use connection::{Auth, Connection, Security};
pub use error::Error;
pub use secret::SecretString;

pub use kafka_protocol::messages::{BrokerId, TopicName};
pub use kafka_protocol::protocol::StrBytes;

#[cfg(test)]
mod tests;
