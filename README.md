# kafka-client

A pure-Rust, async-first Apache Kafka client built on [tokio](https://tokio.rs/) and the [`kafka-protocol`](https://crates.io/crates/kafka-protocol) crate.

## Status

Early stage. Not ready for production use.

Implemented:

- Connection management with retry
- Cluster topology discovery
- Metadata requests
- SASL authentication
- TLS (via `rustls`)
- Topic creation

Not yet implemented:

- Producer
- Consumer / consumer groups

## Goals

- **Async-first.** All I/O is non-blocking and runs on tokio.
- **Pure Rust.** No FFI, no `librdkafka` dependency. Wire protocol encoding and decoding go through the `kafka-protocol` crate, which is auto-generated from Kafka's JSON schema and supports every API key and version.
- **Protocol-driven.** Follows the Apache Kafka protocol specification directly.

## Requirements

- Rust 1.94+ (edition 2024)
- Docker (for running the test suite)

## Build & test

```bash
cargo build
cargo test
```

Tests use [`testcontainers`](https://crates.io/crates/testcontainers) with the `apache/kafka:3.7.0` image and require a running Docker daemon. Broker containers are shared across tests (one per security mode) and started lazily on first use.

## License

MIT. See [LICENSE](LICENSE).
