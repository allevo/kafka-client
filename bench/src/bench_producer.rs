//! The `BenchProducer` trait and the shared runner loop — the fairness
//! backbone of the harness. Every crate under test is driven through the
//! exact same loop; only the `send_one` / `finalize` bodies differ.

use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;

use crate::workload::{LatencyStats, RunResult, Workload};

/// One crate's producer, adapted to the harness.
///
/// The generic (no `dyn`, no `async-trait`) form is deliberate: `main`
/// matches on the `CrateId` to pick the concrete type and the runner is
/// monomorphised per crate, so there is zero dispatch overhead on the hot
/// `send_one` path.
pub trait BenchProducer {
    /// Issue one record and resolve to its send→ack latency.
    ///
    /// The impl must capture `Instant::now()` immediately before handing
    /// the record to its client, and resolve the returned future **only**
    /// once the broker has acked it at the configured `acks` level (for
    /// `acks=None`, once the bytes are on the wire — there is no ack).
    fn send_one(
        &self,
        payload: Bytes,
    ) -> impl std::future::Future<Output = anyhow::Result<Duration>> + Send;

    /// Flush every buffered record and tear the producer down. Called once
    /// after the measured phase; its cost is folded into the throughput
    /// clock (the clock stops only after `finalize` returns) but it
    /// contributes no latency samples.
    fn finalize(self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

/// Histogram bounds: 1 µs .. 60 s, 3 significant figures. A sample outside
/// this range is clamped rather than dropped — see `record_us`.
const HIST_LOW_US: u64 = 1;
const HIST_HIGH_US: u64 = 60_000_000;

fn new_histogram() -> anyhow::Result<Histogram<u64>> {
    Ok(Histogram::new_with_bounds(HIST_LOW_US, HIST_HIGH_US, 3)?)
}

/// Record a latency sample, clamped into the histogram's representable
/// range. Sub-microsecond samples (possible for `acks=None`) floor to 1 µs;
/// anything past 60 s saturates rather than erroring out the whole run.
fn record_us(hist: &mut Histogram<u64>, d: Duration) {
    let us = (d.as_micros() as u64).clamp(HIST_LOW_US, HIST_HIGH_US);
    hist.record(us)
        .expect("clamped value is always in histogram range");
}

/// Whether the current phase should keep issuing records.
enum Limit {
    Records(u64),
    Duration(Duration),
}

/// Issue records through `producer` at a fixed outstanding-ack depth.
///
/// `FuturesUnordered` holds the in-flight `send_one` futures; capping its
/// length at `in_flight` *is* the depth control — no semaphore or task
/// spawning needed, because the futures are polled in place on this task
/// rather than detached. Returns the number of records that completed.
async fn run_phase<P: BenchProducer>(
    producer: &P,
    payload: &Bytes,
    in_flight: usize,
    limit: Limit,
    hist: &mut Histogram<u64>,
) -> anyhow::Result<u64> {
    let mut futs = FuturesUnordered::new();
    let mut issued: u64 = 0;
    let mut completed: u64 = 0;
    let phase_start = Instant::now();

    loop {
        // Drain back down to the depth cap before issuing more. While the
        // cap is hit this is where the task parks, polling the in-flight
        // sends until one acks.
        while futs.len() >= in_flight {
            let latency = futs
                .next()
                .await
                .expect("futs is non-empty while len >= in_flight")?;
            record_us(hist, latency);
            completed += 1;
        }

        let keep_going = match limit {
            Limit::Records(n) => issued < n,
            Limit::Duration(d) => phase_start.elapsed() < d,
        };
        if !keep_going {
            break;
        }

        futs.push(producer.send_one(payload.clone()));
        issued += 1;
    }

    // Drain whatever is still outstanding after the issue loop stops.
    while let Some(res) = futs.next().await {
        record_us(hist, res?);
        completed += 1;
    }

    Ok(completed)
}

/// Run one full `(record size, crate)` cell: warmup (discarded), then the
/// measured phase, then `finalize`. The throughput clock starts right
/// before the first measured send and stops only after the last ack
/// resolves *and* `finalize` returns.
pub async fn run<P: BenchProducer>(
    producer: P,
    workload: &Workload,
    payload: Bytes,
) -> anyhow::Result<RunResult> {
    // Warmup: connections, broker metadata, allocator / `zeropool` pools.
    // Samples and timing are thrown away.
    if workload.warmup_records > 0 {
        let mut warmup_hist = new_histogram()?;
        run_phase(
            &producer,
            &payload,
            workload.in_flight,
            Limit::Records(workload.warmup_records),
            &mut warmup_hist,
        )
        .await?;
    }

    let mut hist = new_histogram()?;
    let limit = match workload.duration {
        Some(d) => Limit::Duration(d),
        None => Limit::Records(workload.record_count),
    };

    let measured_start = Instant::now();
    let completed = run_phase(&producer, &payload, workload.in_flight, limit, &mut hist).await?;
    producer.finalize().await?;
    let elapsed = measured_start.elapsed();

    let secs = elapsed.as_secs_f64().max(1e-9);
    let records_per_s = completed as f64 / secs;
    let mb_per_s = (completed as f64 * workload.record_size as f64) / 1e6 / secs;

    Ok(RunResult {
        records_per_s,
        mb_per_s,
        latency: LatencyStats {
            p50: us_to_ms(hist.value_at_quantile(0.50)),
            p99: us_to_ms(hist.value_at_quantile(0.99)),
            p999: us_to_ms(hist.value_at_quantile(0.999)),
            max: us_to_ms(hist.max()),
        },
    })
}

fn us_to_ms(us: u64) -> f64 {
    us as f64 / 1000.0
}
