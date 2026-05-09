pub mod admin;
pub mod broker;
pub mod client;
pub mod config;
pub mod consumer;
pub mod connection;
pub mod error;
pub mod producer;
pub mod secret;

pub use admin::AdminClient;
pub use broker::{Auth, BrokerClient, ReauthDelayFn, ResponseFuture, default_reauth_delay};
pub use client::{CallOptions, Client, ClientResponseFuture, NodeTarget, PartitionId};
pub use config::{Config, Security};
pub use consumer::{
    Consumer, ConsumerConfig, ConsumerRecord, IsolationLevel, OffsetReset, TopicPartition,
};
pub use connection::Connection;
pub use error::Error;
pub use producer::{Acks, Producer, ProducerConfig, ProducerRecord, RecordMetadata, SendFuture};
pub use secret::SecretString;

pub use kafka_protocol::messages::{BrokerId, TopicName};
pub use kafka_protocol::protocol::StrBytes;
pub use kafka_protocol::records::Compression;

#[cfg(test)]
mod tests;
