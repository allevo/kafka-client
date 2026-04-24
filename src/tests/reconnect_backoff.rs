//! Tests for the per-broker reconnect backoff gate (CONNECT_RETRY.md /
//! TIMEOUT.md §1.2). Each test pins a specific rule from the plan.
//!
//! # Observation model
//!
//! The dialer matches Java's `NetworkClient` semantics: a failed dial
//! never wakes parked waiters — the dialer logs, bumps the failure
//! counter, and loops into its backoff gate. Calls to `broker(id)` stay
//! pending across any number of failed cycles until (a) one dial
//! succeeds, (b) `close()` aborts the dialer, (c) the id drops out of
//! metadata, or (d) the caller imposes its own timeout.
//!
//! Tests therefore cannot observe gate timing via caller-side error
//! latency. Instead, the proxy stores an `Instant` for every dial
//! attempt it sees; tests compare adjacent timestamps against the
//! nominal backoff schedule.
//!
//! # Setup pattern
//!
//! Dial the real broker directly for bootstrap, then use
//! `replace_metadata_for_test` to publish a *synthetic* broker-id whose
//! address points at a `proxy::start` endpoint. The first `client.broker(
//! synthetic_id)` call then goes cold-path through the gate and the proxy,
//! while the bootstrap slot stays out of the way. We use a synthetic id
//! (rather than mutating the real broker's slot) because the bootstrap
//! slot already holds a live `BrokerClient`; the gate tests need a
//! guaranteed cold slot.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kafka_protocol::messages::{ApiKey, BrokerId};

use super::helpers;
use super::helpers::proxy::{Fault, FaultPlan, ProxyHandle, RequestView};
use crate::client::next_backoff;

/// Build a `FaultPlan` that drops every TCP connection during the
/// handshake. Bumps `counter` once per connection (ApiVersions is the
/// first frame of `BrokerClient::new`'s handshake).
fn drop_all_plan(counter: Arc<AtomicUsize>) -> FaultPlan {
    FaultPlan {
        on_request: Some(Arc::new(move |view: &RequestView<'_>| {
            if view.api_key == ApiKey::ApiVersions {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            Some(Fault::DropConnection)
        })),
        on_response: None,
    }
}

/// Build a `FaultPlan` that drops every TCP connection during the
/// handshake and records the `Instant` of each attempt. Used by timing
/// tests that compare adjacent dial attempts against the nominal
/// backoff schedule (failures no longer propagate to the caller, so the
/// proxy log is the only observable side-channel).
fn timing_drop_plan(times: Arc<Mutex<Vec<Instant>>>) -> FaultPlan {
    FaultPlan {
        on_request: Some(Arc::new(move |view: &RequestView<'_>| {
            if view.api_key == ApiKey::ApiVersions {
                times.lock().unwrap().push(Instant::now());
            }
            Some(Fault::DropConnection)
        })),
        on_response: None,
    }
}

/// Dial the real broker, then publish a synthetic broker id (`SYNTH_ID`)
/// whose address is the proxy's ephemeral port. The bootstrap broker
/// (real id) stays in metadata too so `any_broker` and `refresh_metadata`
/// still work if the test needs them.
const SYNTH_ID: BrokerId = BrokerId(9999);

async fn client_through_proxy(proxy: &ProxyHandle, base: Duration, max: Duration) -> crate::Client {
    let broker = helpers::plaintext_broker().await;
    let bootstrap = [crate::Config::new(&broker.host, broker.port)
        .with_reconnect_backoff(base)
        .with_reconnect_backoff_max(max)];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    // Publish the synthetic id alongside the real broker. Keep the real
    // broker in metadata so `any_broker` / `refresh_metadata` remain
    // usable, but leave SYNTH_ID pointing at the proxy so cold-path
    // dials there funnel through the fault injector.
    let controller = client.controller_id();
    client.replace_metadata_for_test(
        controller,
        vec![
            (controller, broker.host.clone(), broker.port),
            (SYNTH_ID, proxy.host.clone(), proxy.port),
        ],
        vec![],
    );
    client
}

// ---------------------------------------------------------------------------
// 8. Jitter bounds — unit-level, no broker needed.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn backoff_formula_stays_within_jitter_bounds() {
    let base = Duration::from_millis(100);
    let max = Duration::from_millis(1_000);

    for attempts in 1u32..=8 {
        let shift = (attempts - 1).min(31);
        let nominal = base
            .as_nanos()
            .saturating_mul(1u128 << shift)
            .min(max.as_nanos()) as u64;
        let low = (nominal as f64 * 0.8) as u64;
        let high = (nominal as f64 * 1.2) as u64 + 1;

        for _ in 0..200 {
            let d = next_backoff(attempts, base, max);
            let got = d.as_nanos() as u64;
            assert!(
                got >= low && got <= high,
                "attempts={attempts}: {got} ns not in [{low}, {high}] (nominal {nominal})"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 1. First-failure gate arms.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn gate_arms_after_first_failure() {
    let broker = helpers::plaintext_broker().await;
    let times = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let proxy =
        helpers::proxy::start(&broker.host, broker.port, timing_drop_plan(times.clone())).await;

    let base = Duration::from_millis(500);
    let client = client_through_proxy(&proxy, base, Duration::from_secs(10)).await;

    // Background `broker()` call — stays pending across failed cycles
    // under the Java-semantic retry model. We observe dial cadence via
    // `timing_drop_plan`'s timestamp log, not via caller-side errors.
    let client_bg = client.clone();
    let task = tokio::spawn(async move { client_bg.broker(SYNTH_ID).await });

    // Window: 1st dial fires immediately; 2nd is gated by ~base (500 ms).
    // 950 ms comfortably captures both without risking a 3rd.
    tokio::time::sleep(Duration::from_millis(950)).await;

    client.close();
    // close() aborts the dialer; the parked waiter wakes with Err.
    let _ = task.await;

    let ts = times.lock().unwrap().clone();
    assert!(
        ts.len() >= 2,
        "expected >= 2 dial attempts in window, got {}",
        ts.len()
    );
    let gap = ts[1].duration_since(ts[0]);
    assert!(
        gap >= Duration::from_millis(350),
        "gap too short: {gap:?} (expected >= 350ms)"
    );
    assert!(
        gap <= Duration::from_millis(1_500),
        "gap too long: {gap:?} (expected <= 1500ms)"
    );
}

// ---------------------------------------------------------------------------
// 2. Exponential growth across repeated failures.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn exponential_growth_caps_at_max() {
    let broker = helpers::plaintext_broker().await;
    let times = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let proxy =
        helpers::proxy::start(&broker.host, broker.port, timing_drop_plan(times.clone())).await;

    let base = Duration::from_millis(100);
    let max = Duration::from_millis(1_000);
    let client = client_through_proxy(&proxy, base, max).await;

    let client_bg = client.clone();
    let task = tokio::spawn(async move { client_bg.broker(SYNTH_ID).await });

    // Nominal pre-dial sleeps across calls: 0, 100, 200, 400, 800, 1000.
    // Cumulative start times of dial attempts 1..=7:
    //   t0=0, 100, 300, 700, 1500, 2500, 3500 ms
    // Wait ~4.5 s to capture at least 7 dial attempts.
    tokio::time::sleep(Duration::from_millis(4_500)).await;

    client.close();
    let _ = task.await;

    let ts = times.lock().unwrap().clone();
    assert!(
        ts.len() >= 7,
        "expected >= 7 dial attempts, got {}",
        ts.len()
    );

    // Adjacent gaps reflect the pre-dial sleep for attempt (i+1).
    // gap[0] = sleep before attempt 2 = base (100 ms nominal).
    let nominal = [100u64, 200, 400, 800, 1000, 1000];

    for (i, nom) in nominal.iter().enumerate() {
        let gap = ts[i + 1].duration_since(ts[i]);
        // Lower bound: 70 % of nominal.
        let low = Duration::from_millis((*nom as f64 * 0.7) as u64);
        // Upper bound: 150 % of nominal + 200 ms scheduler slack.
        let high = Duration::from_millis((*nom as f64 * 1.5) as u64 + 200);
        assert!(
            gap >= low && gap <= high,
            "dial {}: gap {gap:?} not in [{low:?}, {high:?}] (nominal {nom}ms)",
            i + 2
        );
    }
}

// ---------------------------------------------------------------------------
// 3. Successful dial resets state.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn success_resets_backoff_state() {
    let broker = helpers::plaintext_broker().await;
    let times = Arc::new(Mutex::new(Vec::<Instant>::new()));
    let proxy =
        helpers::proxy::start(&broker.host, broker.port, timing_drop_plan(times.clone())).await;

    let base = Duration::from_millis(300);
    let client = client_through_proxy(&proxy, base, Duration::from_secs(10)).await;

    // Let the first dial fail, then swap to a healthy plan so the
    // gated retry succeeds and resets the internal failure counter.
    let client_bg = client.clone();
    let task = tokio::spawn(async move { client_bg.broker(SYNTH_ID).await });

    // Wait for the first failed attempt to land, then flip to healthy.
    // The dialer's failure log is the exact signal we're fencing on —
    // a raw sleep is both flaky (150ms is too short on a loaded CI host)
    // and wasteful (fast hosts would already be deep into the backoff
    // sleep). Log-wait fires within a ms of the actual failure.
    helpers::wait_for_log(
        || logs_contain("broker dial failed; will retry after backoff"),
        Duration::from_secs(2),
        "first dial attempt never failed",
    )
    .await;
    proxy.set_plan(FaultPlan::new());

    // The next attempt is gated by `base` (~300 ms) and should succeed.
    let r = tokio::time::timeout(Duration::from_secs(3), task).await;
    let join = r.expect("dialer did not produce a BrokerClient in time");
    let _ = join.expect("task panicked").expect("broker() returned Err");

    // Now flip back to drop-all with a FRESH timestamp log and trip the
    // cached client into shutdown via a failing request.
    let times2 = Arc::new(Mutex::new(Vec::<Instant>::new()));
    proxy.set_plan(timing_drop_plan(times2.clone()));
    let cached = client.broker(SYNTH_ID).await.expect("still cached");
    let _ = cached.fetch_metadata().await;
    drop(cached);
    // Wait for read_task to actually observe the drop and exit before
    // the next broker() call — otherwise the fast path can still return
    // the stale Slot::Resolved and we never exercise the eviction path.
    helpers::wait_for_log(
        || logs_contain("read task exiting"),
        Duration::from_secs(2),
        "read_task did not exit after cached client was dropped/failed",
    )
    .await;

    // Next `broker()` call observes shutdown, evicts, and schedules a
    // dial gated by `base` (initial_times = 1 respawn path). If the
    // failure counter hadn't reset, this would sleep `2*base`/`4*base`.
    let t0 = Instant::now();
    let client_bg2 = client.clone();
    let task2 = tokio::spawn(async move { client_bg2.broker(SYNTH_ID).await });

    // Long enough to catch one dial attempt gated by `base`.
    tokio::time::sleep(base + Duration::from_millis(400)).await;

    client.close();
    let _ = task2.await;

    let ts2 = times2.lock().unwrap().clone();
    assert!(
        !ts2.is_empty(),
        "expected at least one dial attempt after shutdown eviction"
    );
    let gap = ts2[0].duration_since(t0);
    assert!(
        gap >= Duration::from_millis(200),
        "gap too short, reset didn't gate: {gap:?}"
    );
    assert!(
        gap <= Duration::from_millis(900),
        "gap too long — backoff did not reset to base: {gap:?}"
    );
}

// ---------------------------------------------------------------------------
// 5. Single-flight: concurrent callers collapse into one dialer and
//    all see the eventual success.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn concurrent_callers_single_flight() {
    let broker = helpers::plaintext_broker().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let proxy =
        helpers::proxy::start(&broker.host, broker.port, drop_all_plan(counter.clone())).await;

    let base = Duration::from_millis(200);
    let client = client_through_proxy(&proxy, base, Duration::from_secs(10)).await;

    // 10 concurrent callers. Under Java semantics they all park on the
    // same dialer inbox and stay pending across failed cycles.
    let mut handles = Vec::with_capacity(10);
    for _ in 0..10 {
        let c = client.clone();
        handles.push(tokio::spawn(async move { c.broker(SYNTH_ID).await }));
    }

    // Let the dialer fail at least once, then flip to healthy.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        counter.load(Ordering::SeqCst) >= 1,
        "expected at least one failed dial attempt before swap, got {}",
        counter.load(Ordering::SeqCst)
    );
    proxy.set_plan(FaultPlan::new());

    // Every waiter must see `Ok` once the next gated dial succeeds.
    for (i, h) in handles.into_iter().enumerate() {
        let r = tokio::time::timeout(Duration::from_secs(5), h)
            .await
            .unwrap_or_else(|_| panic!("waiter {i} did not complete in time"))
            .expect("join");
        assert!(r.is_ok(), "waiter {i} should see Ok after dial succeeds");
    }

    // The whole point of the test: 10 concurrent callers must collapse
    // into a single dialer. `spawn_dialer` logs once per task, so exactly
    // one spawn line for SYNTH_ID proves every follow-up caller parked on
    // the shared inbox. Without this assertion the test passes even if
    // each caller spun up its own dialer.
    logs_assert(|lines: &[&str]| {
        let spawns = lines
            .iter()
            .filter(|l| {
                l.contains("spawning broker dialer task")
                    && l.contains(&format!("node_id={}", SYNTH_ID.0))
            })
            .count();
        if spawns == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly 1 dialer spawn for SYNTH_ID, got {spawns}"
            ))
        }
    });
}

// ---------------------------------------------------------------------------
// 6. Single dead broker does not starve `any_broker()`.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn dead_broker_does_not_starve_any_broker() {
    let broker = helpers::plaintext_broker().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let proxy =
        helpers::proxy::start(&broker.host, broker.port, drop_all_plan(counter.clone())).await;

    let base = Duration::from_millis(500);
    let max = Duration::from_millis(2_000);
    let client = client_through_proxy(&proxy, base, max).await;

    // Background `broker(SYNTH_ID)` — puts the synthetic slot into
    // `Slot::Dialing` and arms its gate. We never await it; `close()`
    // at the end releases it.
    let client_bg = client.clone();
    let dead_task = tokio::spawn(async move { client_bg.broker(SYNTH_ID).await });

    // The spawn log fires from inside `spawn_dialer`, called under the
    // same map mutex that installs `Slot::Dialing` — so by the time
    // this log is visible, the Dialing slot is guaranteed in place.
    // Replaces the old `sleep(100ms)` that was both flaky under load
    // and wasteful on fast hosts. SYNTH_ID is the only cold dial in
    // this test (bootstrap went directly to `Slot::Resolved`), so a
    // bare message match is unambiguous.
    helpers::wait_for_log(
        || logs_contain("spawning broker dialer task"),
        Duration::from_secs(2),
        "dialer for SYNTH_ID never spawned",
    )
    .await;

    // 100 tight-loop any_broker calls should all return the bootstrap
    // broker and complete well under `max`.
    let t0 = Instant::now();
    for _ in 0..100 {
        client.any_broker().await.expect("healthy broker available");
    }
    let elapsed = Instant::now().duration_since(t0);
    assert!(
        elapsed < max,
        "any_broker starved by dead broker: {elapsed:?}"
    );

    client.close();
    let _ = dead_task.await;
}

// ---------------------------------------------------------------------------
// 7. Gate does not stall `close()` / concurrent paths.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn close_does_not_block_on_sleeping_gate() {
    let broker = helpers::plaintext_broker().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let proxy =
        helpers::proxy::start(&broker.host, broker.port, drop_all_plan(counter.clone())).await;

    let base = Duration::from_secs(5);
    let client = client_through_proxy(&proxy, base, Duration::from_secs(10)).await;

    // Task A starts a `broker()` call. Its first dial fails fast; the
    // dialer then parks in a ~5 s gate sleep.
    let client_a = client.clone();
    let task_a = tokio::spawn(async move {
        // We don't care about the result — close() releases the parked
        // waiter with a ProtocolError. The assertion below is on
        // close's return latency, not on A.
        let _ = client_a.broker(SYNTH_ID).await;
    });

    // Wait until the dialer has actually entered its gate sleep. The
    // failure log fires right before the backoff sleep on the next
    // iteration, so this is a tight lower bound: by the time the log is
    // visible, the dialer is either already parked in `sleep(base)` or
    // about to be on the next poll. `base` is 5 s so there is no risk of
    // the dialer waking and firing a second attempt before `close()`.
    helpers::wait_for_log(
        || logs_contain("broker dial failed; will retry after backoff"),
        Duration::from_secs(2),
        "dialer never reached its gate sleep",
    )
    .await;

    let t0 = Instant::now();
    client.close();
    let close_elapsed = Instant::now().duration_since(t0);

    assert!(
        close_elapsed < Duration::from_millis(200),
        "close() stalled behind gate sleep: {close_elapsed:?}"
    );

    // close() aborted the dialer; parked senders dropped; A wakes with Err.
    let _ = task_a.await;
}

// ---------------------------------------------------------------------------
// 9. Java-semantic rule: dial failures never wake parked waiters.
// ---------------------------------------------------------------------------

#[tokio::test]
#[tracing_test::traced_test]
async fn failing_dial_does_not_wake_waiters() {
    let broker = helpers::plaintext_broker().await;
    let counter = Arc::new(AtomicUsize::new(0));
    let proxy =
        helpers::proxy::start(&broker.host, broker.port, drop_all_plan(counter.clone())).await;

    let base = Duration::from_millis(200);
    let client = client_through_proxy(&proxy, base, Duration::from_secs(10)).await;

    // Spawn a `broker()` call against a permanently-dead broker. It
    // must stay `Pending` through multiple failed dial cycles.
    let client_bg = client.clone();
    let task = tokio::spawn(async move { client_bg.broker(SYNTH_ID).await });

    // Window wide enough for 2+ dial attempts: dial 1 at t≈0, dial 2 at
    // t≈base (200 ms), dial 3 at t≈3*base (600 ms).
    tokio::time::sleep(Duration::from_millis(800)).await;

    assert!(
        !task.is_finished(),
        "waiter woke up despite no successful dial"
    );
    assert!(
        counter.load(Ordering::SeqCst) >= 2,
        "expected at least 2 dial attempts, got {}",
        counter.load(Ordering::SeqCst)
    );

    // `close()` is the only way to release a parked waiter on a
    // permanently-dead broker (besides caller-side timeout).
    client.close();
    let r = task.await.expect("join");
    assert!(
        r.is_err(),
        "expected Err after close(), got Ok from permanently-dead broker"
    );
}

// ---------------------------------------------------------------------------
// 10. `any_broker()` parks on a dialer when ALL brokers are `Dialing`.
// ---------------------------------------------------------------------------

/// Regression test for Bug 3: when every known broker is mid-dial,
/// `any_broker()` must wait on one in-progress dialer rather than
/// returning `NoBrokerAvailable` immediately.
#[tokio::test]
#[tracing_test::traced_test]
async fn any_broker_parks_when_all_brokers_dialing() {
    const SYNTH_A: BrokerId = BrokerId(9990);
    const SYNTH_B: BrokerId = BrokerId(9991);

    let broker = helpers::plaintext_broker().await;
    let counter = Arc::new(AtomicUsize::new(0));

    // Both proxies drop every connection initially.
    let proxy_a =
        helpers::proxy::start(&broker.host, broker.port, drop_all_plan(counter.clone())).await;
    let proxy_b =
        helpers::proxy::start(&broker.host, broker.port, drop_all_plan(counter.clone())).await;

    let base = Duration::from_millis(200);
    let max = Duration::from_millis(1_000);

    let bootstrap = [crate::Config::new(&broker.host, broker.port)
        .with_reconnect_backoff(base)
        .with_reconnect_backoff_max(max)];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    // Replace metadata with ONLY synthetic IDs. The bootstrap
    // connection is pruned — no Resolved slot survives.
    client.replace_metadata_for_test(
        SYNTH_A,
        vec![
            (SYNTH_A, proxy_a.host.clone(), proxy_a.port),
            (SYNTH_B, proxy_b.host.clone(), proxy_b.port),
        ],
        vec![],
    );

    // Install Slot::Dialing for both synthetic brokers. The drop-all
    // proxies ensure the dialers fail and enter backoff, keeping the
    // slots in Dialing state.
    let client_a = client.clone();
    let task_a = tokio::spawn(async move { client_a.broker(SYNTH_A).await });
    let client_b = client.clone();
    let task_b = tokio::spawn(async move { client_b.broker(SYNTH_B).await });

    // Wait for both Dialing slots to be installed and the first dial
    // attempt of each to fail — so both are parked in their backoff
    // sleeps when we flip the proxies below. Two failures (one per
    // broker) is what "all brokers dialing" means in the fix's fallback
    // branch; if we flip before the second one has failed, any_broker
    // could find a `Slot::Dialing` that hasn't issued its first dial yet
    // and the assertion on "parked on dialer" still passes but for the
    // wrong reason. The shared proxy counter is bumped once per
    // ApiVersions seen (see `drop_all_plan`), so counter >= 2 means
    // each dialer has issued at least one attempt (with base=200ms, the
    // 9990/9991 dialers start near-simultaneously and the second
    // dialer's first attempt is almost always interleaved before the
    // first dialer's second attempt).
    helpers::wait_for_log(
        || counter.load(Ordering::SeqCst) >= 2,
        Duration::from_secs(3),
        "both dialers did not reach their first failure",
    )
    .await;

    // Unblock both proxies so the next retry cycle succeeds.
    // any_broker() parks on whichever dialer it encounters first
    // (HashMap iteration order), so both must be healthy.
    proxy_a.set_plan(FaultPlan::new());
    proxy_b.set_plan(FaultPlan::new());

    // any_broker() should park on one dialer's inbox rather than
    // returning NoBrokerAvailable. The dialer succeeds through the
    // now-healthy proxy within a few backoff cycles.
    let t0 = Instant::now();
    let result = tokio::time::timeout(Duration::from_secs(10), client.any_broker()).await;
    let elapsed = t0.elapsed();

    let broker_client = result
        .expect("any_broker timed out — did not park on dialer")
        .expect("any_broker returned error instead of parking on dialer");

    assert!(
        !broker_client.is_shutdown(),
        "any_broker returned a shut-down client"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "any_broker took too long: {elapsed:?}"
    );

    // The fix's fallback path must have fired: any_broker() saw all
    // slots as Dialing and parked on one rather than returning an error.
    assert!(logs_contain(
        "all brokers dialing, parking on in-progress dial"
    ));

    // Both dialers must have failed at least once (the drop-all phase)
    // before the proxies were switched to pass-through.
    logs_assert(|lines: &[&str]| {
        let fail_count = lines
            .iter()
            .filter(|l| l.contains("broker dial failed; will retry after backoff"))
            .count();
        if fail_count >= 2 {
            Ok(())
        } else {
            Err(format!(
                "expected at least 2 dial failures (one per broker), got {fail_count}"
            ))
        }
    });

    client.close();
    let _ = task_a.await;
    let _ = task_b.await;
}
