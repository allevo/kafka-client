use std::time::Duration;

use kafka_client::{Auth, Client, Config, Security};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::Kafka;

// Integration test (not a unit test under `src/tests/`) because it needs an
// isolated tokio runtime: each `tests/*.rs` file compiles to its own binary,
// so `RuntimeMetrics::num_alive_tasks()` here reflects only tasks this test
// spawned — unlike the shared unit-test runtime where other tests would
// contaminate the count.
#[tokio::test]
async fn task_count_rises_on_connect_and_drops_on_close() {
    // Dedicated broker for this test binary; can't reuse the `LazyLock`
    // shared-broker cache from `src/tests/helpers/shared.rs` because that
    // module is private to the library's test harness.
    let kafka = Kafka::default().start().await.unwrap();
    let host = kafka.get_host().await.unwrap().to_string();
    let port = kafka.get_host_port_ipv4(9092).await.unwrap();

    let metrics = tokio::runtime::Handle::current().metrics();

    // Baseline: `num_alive_tasks()` counts *spawned* tasks only. The
    // `#[tokio::test]` body runs via `Runtime::block_on`, which is the root
    // future and does not count. Any tasks testcontainers spawned during
    // `start()` have already completed by the time it returns.
    let baseline = metrics.num_alive_tasks();
    assert_eq!(
        baseline, 0,
        "expected no spawned tasks alive before connect, got {baseline}"
    );

    let bootstrap = [Config::new(&host, port)];
    let client = Client::connect(&bootstrap, Security::Plaintext, Auth::None)
        .await
        .unwrap();

    let after_connect = metrics.num_alive_tasks();

    // `BrokerClient::new` spawns `write_task` + `read_task` per connection.
    // The conditional `reauth_task` is NOT spawned here: plaintext + no-auth
    // returns `session_lifetime = None`, so no KIP-368 reauth loop is needed.
    assert_eq!(
        after_connect, 2,
        "expected 2 alive tasks after connect (write_task + read_task), got {after_connect}"
    );

    // `close()` is synchronous for the pool but the background tasks exit
    // asynchronously once their shutdown notifications fire. Poll briefly —
    // same pattern as `src/tests/close.rs`.
    client.close();

    let mut attempts = 0;
    while metrics.num_alive_tasks() > 0 && attempts < 50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        attempts += 1;
    }

    let after_close = metrics.num_alive_tasks();
    assert_eq!(
        after_close, 0,
        "expected all spawned tasks to exit after close(), got {after_close}"
    );
}
