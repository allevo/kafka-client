# kafka-client

Pure async Rust implementation of the Apache Kafka protocol. No C bindings, no librdkafka — built from scratch on top of tokio.

## Project status

Early stage. The test infrastructure (testcontainers-based Kafka brokers) is in place. The client library itself is not yet implemented — `src/` currently contains only a placeholder `main.rs` that should be replaced with `lib.rs` once implementation begins.

## Architecture goals

- **Async-first**: Built on tokio. All I/O is non-blocking.
- **Pure Rust**: Implements the Kafka wire protocol directly. No FFI, no librdkafka dependency.
- **Protocol-driven**: Follows the Apache Kafka protocol specification. Each API key (Produce, Fetch, Metadata, etc.) maps to request/response types with versioned serialization.

## Build & test

```bash
cargo build                # build the library
cargo test                 # run all tests (requires Docker)
cargo test --test standalone   # single-broker plaintext test
cargo test --test cluster      # 3-node plaintext cluster test
cargo test --test standalone_tls  # single-broker TLS test
cargo test --test cluster_tls     # 3-node TLS cluster test
```

Tests use `testcontainers` with the `apache/kafka:3.7.0` image. Docker must be running. Broker startup timeout is 120 seconds.

## Rust toolchain

- **Edition**: 2024
- **Minimum rustc**: 1.94+
- **Async runtime**: tokio (full features in dev-dependencies; will move to main dependencies)

## Project layout

```
src/              # Library source (not yet implemented)
tests/
  common/mod.rs   # Shared test helpers: broker configs (plaintext, TLS, standalone, cluster)
  standalone.rs   # Single-broker plaintext integration test
  standalone_tls.rs  # Single-broker TLS integration test
  cluster.rs      # 3-node plaintext cluster integration test
  cluster_tls.rs  # 3-node TLS cluster integration test
  fixtures/secrets/  # JKS keystores and truststore credentials for TLS tests
```

## Conventions

- Integration tests go in `tests/`, one file per scenario. Shared test infrastructure lives in `tests/common/mod.rs`.
- TLS test fixtures (JKS keystores, credential files) live in `tests/fixtures/secrets/` and are copied into containers at runtime.
- Cluster tests use a naming prefix (`"pt"` for plaintext, `"tls"` for TLS) and Docker networks to allow inter-broker communication.
- All test functions are `async` and use `#[tokio::test]`.

## Reference material

- **Kafka source** (`kafka/`): The Apache Kafka Java implementation. Primary reference for protocol behavior, request/response handling, and broker logic. Gitignored — do not commit.
- **librdkafka source** (`librdkafka/`): The C/C++ Kafka client. Reference for how a production client implements connection management, serialization, and error handling. Gitignored — do not commit.
- **Protocol specification**: https://kafka.apache.org/23/ — authoritative docs for the Kafka protocol, wire format, API keys, and broker configuration.

When implementing a protocol feature, cross-reference the Java source in `kafka/` for correctness and `librdkafka/` for client-side design patterns.

## Protocol versioning design

Each API key (Produce, Fetch, etc.) has multiple protocol versions. Newer versions append fields — they never insert fields between existing ones. The client negotiates the version to use by intersecting its supported range with the broker's (via ApiVersionsResponse) and picking the maximum.

Each protocol version gets its own flat struct (e.g. `FetchRequestV0`, `FetchRequestV4`). Fields are duplicated across version structs rather than using `Option` fields or nesting. This keeps each struct self-contained with straightforward serialization — no version-conditional branches, no optional fields. An enum per API key (e.g. `enum FetchRequest { V0(...), V4(...) }`) dispatches to the right variant after version negotiation.

## Important notes

- The Kafka wire protocol uses big-endian encoding. All protocol serialization must use network byte order.
- Kafka protocol versions matter: request/response formats change between API versions. Always be explicit about which version is being implemented.
