use std::collections::HashMap;

use kafka_protocol::messages::BrokerId;

use super::helpers;
use crate::error::Error;

/// Boots a 3-node cluster, dials broker 1 as bootstrap, and waits until
/// metadata reports all 3 nodes (KRaft startup race). Returns the client and
/// a copy of the address map so callers can build synthetic snapshots.
async fn connected_3node_client() -> (crate::Client, HashMap<BrokerId, (String, u16)>) {
    let cluster = helpers::plaintext_cluster().await;
    let addr_map = cluster.addr_map.clone();
    let (ref boot_host, boot_port) = addr_map[&BrokerId(1)];
    let bootstrap = [crate::Config::new(boot_host, boot_port)];

    let resolver_map = addr_map.clone();
    let client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |node_id, _host, _port| Ok(resolver_map[&node_id].clone()),
    )
    .await
    .unwrap();

    let mut metadata = client.refresh_metadata().await.unwrap();
    for _ in 0..10 {
        if metadata.brokers.len() == 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        metadata = client.refresh_metadata().await.unwrap();
    }
    assert_eq!(metadata.brokers.len(), 3);
    (client, addr_map)
}

#[tokio::test]
async fn test_cluster_plaintext() {
    let cluster = helpers::plaintext_cluster().await;

    for (host, port) in cluster.addr_map.values() {
        helpers::assert_tcp_reachable(host, *port).await;
    }
}

#[tokio::test]
async fn test_cluster_client() {
    let cluster = helpers::plaintext_cluster().await;

    let addr_map = cluster.addr_map.clone();
    let (ref boot_host, boot_port) = addr_map[&BrokerId(1)];
    let bootstrap = [crate::Config::new(boot_host, boot_port)];

    let client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |node_id, _host, _port| Ok(addr_map[&node_id].clone()),
    )
    .await
    .unwrap();

    // Brokers may need time to fully register with each other in KRaft mode
    let mut metadata = client.refresh_metadata().await.unwrap();
    for _ in 0..10 {
        if metadata.brokers.len() == 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        metadata = client.refresh_metadata().await.unwrap();
    }
    assert_eq!(metadata.brokers.len(), 3);

    let controller_id = client.controller_id();
    assert!(
        (1..=3).contains(&controller_id.0),
        "controller_id {} not in range 1..=3",
        controller_id.0
    );

    let mut node_ids: Vec<BrokerId> = metadata.brokers.iter().map(|b| b.node_id).collect();
    node_ids.sort_by_key(|id| id.0);
    assert_eq!(node_ids, vec![BrokerId(1), BrokerId(2), BrokerId(3)]);

    // Each broker should eventually see all 3 peers
    for id in [
        client.controller_id(),
        BrokerId(1),
        BrokerId(2),
        BrokerId(3),
    ] {
        let broker = client.broker(id).await.unwrap();
        let mut m = broker.fetch_metadata().await.unwrap();
        for _ in 0..10 {
            if m.brokers.len() == 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            m = broker.fetch_metadata().await.unwrap();
        }
        assert_eq!(
            m.brokers.len(),
            3,
            "broker {} sees wrong broker count",
            id.0
        );
    }
}

/// Exercises the `&self`-only surface of `Client` under concurrency:
/// many tasks racing on `broker(id)` for an unconnected broker (the
/// `OnceCell` per slot must collapse those into a single dial), interleaved
/// with `refresh_metadata()` callers reading/swapping the metadata snapshot.
/// The test passes if every task observes a working `BrokerClient` and the
/// whole thing terminates without panics or deadlocks.
#[tokio::test]
async fn test_cluster_client_concurrent_access() {
    let cluster = helpers::plaintext_cluster().await;

    let addr_map = cluster.addr_map.clone();
    let (ref boot_host, boot_port) = addr_map[&BrokerId(1)];
    let bootstrap = [crate::Config::new(boot_host, boot_port)];

    let client = crate::Client::connect_with_resolver(
        &bootstrap,
        crate::Security::Plaintext,
        crate::Auth::None,
        move |node_id, _host, _port| Ok(addr_map[&node_id].clone()),
    )
    .await
    .unwrap();

    // Wait until metadata reports all 3 brokers (KRaft startup race, same as
    // the existing test).
    let mut metadata = client.refresh_metadata().await.unwrap();
    for _ in 0..10 {
        if metadata.brokers.len() == 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        metadata = client.refresh_metadata().await.unwrap();
    }
    assert_eq!(metadata.brokers.len(), 3);

    // Pick a broker that the bootstrap path did *not* dial. The bootstrap
    // seeds the connection slot for whichever id matches its host:port; the
    // other two ids start out unconnected, so racing on them exercises the
    // OnceCell init path under contention.
    let bootstrap_id = BrokerId(1);
    let target_id = [BrokerId(2), BrokerId(3)]
        .into_iter()
        .find(|id| *id != bootstrap_id)
        .unwrap();

    // Spawn a pile of tasks racing on the same unconnected broker plus a
    // few that hammer refresh_metadata in parallel. Cloning `Client` is
    // cheap (Arc bump) — exactly the ergonomic we want from this refactor.
    let mut handles = Vec::new();
    for _ in 0..16 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let broker = c.broker(target_id).await.unwrap();
            let m = broker.fetch_metadata().await.unwrap();
            assert!(!m.brokers.is_empty());
        }));
    }
    for _ in 0..4 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let m = c.refresh_metadata().await.unwrap();
            assert!(!m.brokers.is_empty());
        }));
    }
    for _ in 0..4 {
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let controller = c.controller().await.unwrap();
            let m = controller.fetch_metadata().await.unwrap();
            assert!(!m.brokers.is_empty());
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

/// `broker(bogus_id)` used to seed an empty `Arc<OnceCell>` slot before the
/// metadata validation rejected it — a caller-driven, unbounded leak. The
/// restructured `broker()` validates against metadata *before* mutating the
/// connection cache, so a bogus call leaves the slot count unchanged.
#[tokio::test]
async fn test_broker_unknown_id_does_not_pollute_connections() {
    let (client, _addr_map) = connected_3node_client().await;

    let before = client.connection_slot_count();
    let result = client.broker(BrokerId(9999)).await;
    assert!(matches!(result, Err(Error::NoBrokerAvailable(_))));
    let after = client.connection_slot_count();
    assert_eq!(
        before, after,
        "bogus broker id leaked a slot into the connection cache"
    );
}

/// A broker that disappears from a refreshed metadata snapshot must also
/// disappear from the connection cache; otherwise the fast path keeps
/// handing out a `BrokerClient` for an id the cluster no longer recognizes.
/// We simulate the disappearance via the test-only metadata replacement
/// accessor, which goes through the same prune-then-publish helper as
/// `refresh_metadata` so the test exercises the production code path.
#[tokio::test]
async fn test_broker_removed_from_metadata_is_pruned() {
    let (client, addr_map) = connected_3node_client().await;

    // Force a slot for broker 2 into the cache.
    let _b2 = client.broker(BrokerId(2)).await.unwrap();
    assert!(client.has_connection_slot(BrokerId(2)));

    // Synthetic metadata: brokers 1 and 3 only. Hard-code the controller as
    // BrokerId(1) so the synthetic snapshot is internally consistent
    // regardless of which node Kafka actually elected.
    let surviving: Vec<(BrokerId, String, u16)> = addr_map
        .iter()
        .filter(|(id, _)| **id != BrokerId(2))
        .map(|(id, (h, p))| (*id, h.clone(), *p))
        .collect();
    client.replace_metadata_for_test(BrokerId(1), surviving);

    assert!(
        !client.has_connection_slot(BrokerId(2)),
        "stale slot for departed broker was not pruned"
    );

    // Subsequent dials for the departed id must fail metadata validation.
    let result = client.broker(BrokerId(2)).await;
    assert!(matches!(result, Err(Error::NoBrokerAvailable(_))));
}

/// When a broker connection silently dies, `read_task` flips the shutdown
/// flag on the cached `BrokerClient`. The fast path used to keep handing
/// that corpse out forever, so every subsequent request would fail with
/// `ConnectionAborted` and there was no way to recover short of dropping
/// the whole `Client`. The fast path now eagerly evicts shutdown slots so
/// the cold path dials a fresh connection.
#[tokio::test]
async fn test_broker_dead_connection_is_replaced() {
    let (client, _addr_map) = connected_3node_client().await;

    let first = client.broker(BrokerId(2)).await.unwrap();
    let first_id = first.id();
    assert!(!first.is_shutdown());

    // Simulate read_task flipping the shutdown flag without actually
    // killing the socket — same observable state, deterministic.
    first.force_shutdown_for_test();
    assert!(first.is_shutdown());

    let second = client.broker(BrokerId(2)).await.unwrap();
    assert!(!second.is_shutdown(), "redial returned a shutdown client");
    assert_ne!(
        first_id,
        second.id(),
        "fast path returned the corpse instead of dialing fresh"
    );

    // Sanity-check that the fresh client is actually usable end-to-end.
    let m = second.fetch_metadata().await.unwrap();
    assert!(!m.brokers.is_empty());
}

/// `any_broker()` walks the connection cache looking for *any* live client.
/// Before the liveness filter, it would happily return a corpse with the
/// same "no recovery" failure mode as the per-id fast path. Force every
/// cached client into shutdown state and assert `any_broker()` falls
/// through to a fresh dial instead.
#[tokio::test]
async fn test_any_broker_skips_dead_connections() {
    let (client, _addr_map) = connected_3node_client().await;

    // Populate the cache for all three brokers, then poison every entry.
    let b1 = client.broker(BrokerId(1)).await.unwrap();
    let b2 = client.broker(BrokerId(2)).await.unwrap();
    let b3 = client.broker(BrokerId(3)).await.unwrap();
    let dead_ids = [b1.id(), b2.id(), b3.id()];
    b1.force_shutdown_for_test();
    b2.force_shutdown_for_test();
    b3.force_shutdown_for_test();

    // Every cached client is a corpse. `any_broker` must skip them all and
    // fall through to `broker(first_id)`, which evicts the corpse for that
    // id and dials fresh.
    let any = client.any_broker().await.unwrap();
    assert!(!any.is_shutdown(), "any_broker handed out a corpse");
    assert!(
        !dead_ids.contains(&any.id()),
        "any_broker returned one of the original (now-dead) clients"
    );
}
