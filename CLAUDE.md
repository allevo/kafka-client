# kafka-client

Pure async Rust Kafka client built on top of tokio. Uses `kafka-protocol` for wire protocol encoding/decoding.

## Project status

Early stage. Connection management, cluster topology, and basic protocol operations (metadata, SASL auth, topic creation) are implemented. Producer and Consumer are not yet implemented.

## Architecture goals

- **Async-first**: Built on tokio. All I/O is non-blocking.
- **Pure Rust**: No FFI, no librdkafka dependency. Protocol encoding/decoding via `kafka-protocol` crate.
- **Protocol-driven**: Follows the Apache Kafka protocol specification. The `kafka-protocol` crate provides auto-generated types for all Kafka API keys with versioned serialization.

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
src/
  lib.rs           # Crate root, public re-exports
  connection.rs    # TCP/TLS/SASL connection handshake
  client.rs        # BrokerClient: async request/response pipeline, typed send()
  cluster.rs       # Client: cluster topology, broker discovery, address resolver
  config.rs        # Config struct (host, port)
  error.rs         # Error enum (Io, ProtocolError, AuthenticationError, ApiError)
  secret.rs        # SecretString (zeroize wrapper)
tests/
  common/mod.rs    # Shared test helpers: broker configs (plaintext, TLS, standalone, cluster)
  standalone.rs    # Single-broker plaintext integration test
  standalone_tls.rs   # Single-broker TLS integration test
  standalone_sasl.rs  # Single-broker SASL/PLAIN integration test
  cluster.rs       # 3-node plaintext cluster integration test
  cluster_tls.rs   # 3-node TLS cluster integration test
  fixtures/secrets/   # JKS keystores and truststore credentials for TLS tests
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

## Protocol encoding

All protocol encoding/decoding uses the `kafka-protocol` crate (auto-generated from Kafka's JSON schema). Key patterns:

- **Typed send**: `BrokerClient::send<Req, Resp>(api_key, api_version, request)` handles header encoding, framing, and response decoding automatically.
- **Version parameter**: The API version is passed to `encode()`/`decode()` — a single struct per message type handles all versions.
- **Builder pattern**: Message types are `#[non_exhaustive]` and constructed via `Default::default()` + `.with_field(value)` chains.
- **Newtypes**: `BrokerId(i32)`, `TopicName(StrBytes)`, etc. Access inner value via `.0` or deref.

## Code comments

When writing or editing code, add inline comments (`//`) where they help a reader **without project context** understand the code. Follow the full guidelines in `.claude/skills/comments/SKILL.md`. Key points:

- Explain **why**, not what. Reference protocol behavior, spec requirements, or external constraints.
- Preempt confusion — if a reader might think "is this a bug?" or "why not do X?", answer that.
- Never restate what the code does. Never add doc comments (`///`) — those are handled separately.
- Most code needs no comment. Only add them where they genuinely help.

## Important notes

- Kafka protocol versions matter: request/response formats change between API versions. Always be explicit about which version is being used.
- `MetadataRequest::default()` sets `topics: Some(vec![])` (no topics). Use `.with_topics(None)` to request all topics.
