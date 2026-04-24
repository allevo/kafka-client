//! Regression test for Bug 1 (FINDINGS.md): orphaned `BrokerClient` when
//! `close()` races with a dialer completing TCP + auth.
//!
//! The race window in production is sub-microsecond (between the dialer's
//! last `.await` and a synchronous `Mutex::lock`), so we use a test hook
//! (`retry::test_hooks`) to pause the dialer at that exact point and
//! remove the slot from the map — simulating what `close()` does — before
//! letting the dialer proceed. The fix ensures the dialer detects the
//! slot is gone and shuts down the connection rather than handing out
//! untracked clients to waiters.

use kafka_protocol::messages::BrokerId;

use super::helpers;
use super::helpers::proxy::FaultPlan;
use crate::client::test_hooks;

const SYNTH_ID: BrokerId = BrokerId(7777);

#[tokio::test]
#[tracing_test::traced_test]
async fn dialer_shuts_down_connection_when_slot_removed_during_dial() {
    let broker = helpers::plaintext_broker().await;
    let proxy = helpers::proxy::start(&broker.host, broker.port, FaultPlan::new()).await;

    let bootstrap = [crate::Config::new(&broker.host, broker.port)];
    let client = crate::Client::connect(&bootstrap, crate::Security::Plaintext, crate::Auth::None)
        .await
        .unwrap();

    let controller = client.controller_id();
    client.replace_metadata_for_test(
        controller,
        vec![
            (controller, broker.host.clone(), broker.port),
            (SYNTH_ID, proxy.host.clone(), proxy.port),
        ],
        vec![],
    );

    // Arm the hook: the dialer will pause after a successful dial but
    // before acquiring the map lock.
    test_hooks::arm(SYNTH_ID);

    // Start a broker() call. This spawns a dialer for SYNTH_ID, which
    // dials through the proxy and succeeds, then pauses at the hook.
    let client_bg = client.clone();
    let waiter = tokio::spawn(async move { client_bg.broker(SYNTH_ID).await });

    // Wait for the dialer to signal that it has completed the dial.
    test_hooks::DIAL_COMPLETE.notified().await;

    // Simulate close()/metadata-pruning: remove the slot without aborting
    // the dialer task. In production, abort() fires but has no effect
    // because the dialer is in synchronous code past its last .await.
    client.remove_slot_without_abort(SYNTH_ID);
    assert!(
        !client.has_connection_slot(SYNTH_ID),
        "slot should be gone after removal"
    );

    // Release the dialer. It will acquire the map lock, see
    // slot_is_ours() == false, call client.shutdown(), and return
    // without sending to the inbox.
    test_hooks::PROCEED_TO_LOCK.notify_one();

    // The waiter should see Err: the inbox senders are dropped without
    // being sent, so the oneshot receiver gets RecvError → NoBrokerAvailable.
    let result = waiter.await.expect("task should not panic");
    assert!(
        result.is_err(),
        "waiter should see Err when slot removed during dial, got Ok"
    );

    // The orphaned connection should have been shut down by the fix.
    // No leaked slot in the map.
    assert!(
        !client.has_connection_slot(SYNTH_ID),
        "no slot should remain for the removed broker"
    );

    // Verify the log message from the fix fires.
    assert!(logs_contain(
        "slot replaced or removed during dial; shutting down orphaned connection"
    ));

    test_hooks::disarm();
    client.close();
}
