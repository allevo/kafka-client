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

## Benchmarks

`bench/` is a separate workspace member (`kafka-client-bench`) that runs a sustained producer-only workload and compares this library against [`rdkafka`](https://crates.io/crates/rdkafka), [`rskafka`](https://crates.io/crates/rskafka), and [`samsa`](https://crates.io/crates/samsa). The comparison crates live only in `bench/Cargo.toml`, so `cargo build` / `cargo test` on the library stay FFI-free — `rdkafka`'s C/cmake toolchain is pulled in only when the bench is built.

The benchmark needs an external broker (you start it); it does not use testcontainers. For example:

```bash
# 1. Start a broker
docker run -p 9092:9092 apache/kafka:3.7.0

# 2. Build the bench (also surfaces rdkafka's C-toolchain need — install cmake + a C compiler if it fails on librdkafka)
cargo build --release -p kafka-client-bench

# 3. Run it (release build only — debug numbers are meaningless)
KAFKA_BOOTSTRAP=localhost:9092 cargo run --release -p kafka-client-bench -- \
    --records 200000 --record-sizes 100,1024,16384,102400 --in-flight 1000 --acks all

# Smoke run
KAFKA_BOOTSTRAP=localhost:9092 cargo run --release -p kafka-client-bench -- \
    --records 5000 --record-sizes 1024 --warmup-records 500
```

It prints one throughput + ack-latency-percentile table per record size, followed by a methodology footer listing every place a crate could not be configured equivalently. Run `cargo run --release -p kafka-client-bench -- --help` for the full list of knobs.

## License

MIT. See [LICENSE](LICENSE).
