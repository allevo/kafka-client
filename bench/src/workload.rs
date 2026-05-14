//! Shared workload description and result types.
//!
//! Every crate under test is handed the *same* [`Workload`] and the *same*
//! runner loop ([`crate::bench_producer`]); each per-crate impl maps this
//! config onto its own knobs. [`RunResult`] is what a single `(record size,
//! crate)` cell produces.

use std::time::Duration;

use crate::producers::CrateId;

/// Acknowledgement level, bench-local so it can be mapped per crate (each
/// crate spells `acks` differently, and some don't expose it at all — see
/// the methodology footer printed by `main`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcksLevel {
    None,
    Leader,
    All,
}

impl AcksLevel {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "none" | "0" => Ok(Self::None),
            "leader" | "1" => Ok(Self::Leader),
            "all" | "-1" => Ok(Self::All),
            other => anyhow::bail!("invalid --acks value '{other}' (expected none|leader|all)"),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::Leader => "Leader",
            Self::All => "All",
        }
    }
}

/// One benchmark cell's worth of configuration. Cloned per `(size, crate)`
/// pair with `topic` resolved to that pair's unique topic name.
#[derive(Clone, Debug)]
pub struct Workload {
    /// `host:port` of the broker, as passed on the wire to each crate.
    pub bootstrap: String,
    /// Topic for this cell — unique per `(size, crate)`, see `main`.
    pub topic: String,
    pub partitions: i32,
    pub record_size: usize,
    /// Measured records per cell. Ignored when `duration` is `Some`.
    pub record_count: u64,
    /// Unmeasured warmup records issued before the clock starts.
    pub warmup_records: u64,
    /// Outstanding-ack depth the runner holds open.
    pub in_flight: usize,
    pub acks: AcksLevel,
    pub linger: Duration,
    pub batch_size_bytes: usize,
    /// When set, the measured phase runs for this long instead of for a
    /// fixed `record_count`.
    pub duration: Option<Duration>,
}

/// Ack-latency percentiles for one cell, in milliseconds.
#[derive(Clone, Copy, Debug)]
pub struct LatencyStats {
    pub p50: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

/// Throughput + latency for one `(record size, crate)` cell.
#[derive(Clone, Copy, Debug)]
pub struct RunResult {
    pub records_per_s: f64,
    pub mb_per_s: f64,
    pub latency: LatencyStats,
}

/// Print one comparison table for a record size: a header line describing
/// the shared config, then one row per crate (or an `ERROR` row when a
/// crate's cell failed).
pub fn print_result_table(
    record_size: usize,
    workload: &Workload,
    results: &[(CrateId, anyhow::Result<RunResult>)],
) {
    let count_desc = match workload.duration {
        Some(d) => format!("duration: {:.0}s", d.as_secs_f64()),
        None => format!("records: {}", workload.record_count),
    };
    println!(
        "\n=== record size: {} B, {}, partitions: {}, in-flight: {}, acks: {} ===",
        record_size,
        count_desc,
        workload.partitions,
        workload.in_flight,
        workload.acks.label(),
    );
    println!(
        "{:<10} {:>14} {:>10} {:>9} {:>9} {:>9} {:>9}",
        "crate", "throughput", "MB/s", "p50", "p99", "p999", "max",
    );
    for (crate_id, result) in results {
        match result {
            Ok(r) => println!(
                "{:<10} {:>10.0} rec/s {:>10.1} {:>7.2}ms {:>7.2}ms {:>7.2}ms {:>7.2}ms",
                crate_id.label(),
                r.records_per_s,
                r.mb_per_s,
                r.latency.p50,
                r.latency.p99,
                r.latency.p999,
                r.latency.max,
            ),
            Err(e) => println!("{:<10} ERROR: {}", crate_id.label(), e),
        }
    }
}
