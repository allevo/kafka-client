//! Producer benchmark harness: a sustained producer-only workload run
//! against an external broker, comparing this library with `rdkafka`,
//! `rskafka`, and `samsa`.
//!
//! See the repo `README.md` ("Benchmarks") for how to run it.

mod admin;
mod bench_producer;
mod producers;
mod workload;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::Bytes;
use clap::Parser;
use kafka_client::{Auth, Client, Config, Security};

use crate::producers::CrateId;
use crate::workload::{AcksLevel, Workload, print_result_table};

/// Compare producer throughput + ack latency across Rust Kafka crates.
#[derive(Parser, Debug)]
#[command(name = "producer-bench")]
struct Args {
    /// Broker bootstrap address (`host:port`).
    #[arg(long, env = "KAFKA_BOOTSTRAP", default_value = "localhost:9092")]
    bootstrap: String,

    /// Crates to exercise: subset of `this,rdkafka,rskafka,samsa`.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "this,rdkafka,rskafka,samsa"
    )]
    crates: Vec<String>,

    /// Record sizes to sweep, in bytes.
    #[arg(long, value_delimiter = ',', default_value = "100,1024,16384,102400")]
    record_sizes: Vec<usize>,

    /// Measured records per `(crate, size)` cell.
    #[arg(long, default_value_t = 200_000)]
    records: u64,

    /// Measured-phase duration in seconds. Overrides `--records` when set.
    #[arg(long)]
    duration: Option<u64>,

    /// Unmeasured warmup records per cell.
    #[arg(long, default_value_t = 10_000)]
    warmup_records: u64,

    /// Prefix for the per-cell topic names.
    #[arg(long, default_value = "producer-bench")]
    topic_prefix: String,

    /// Partition count for each created topic.
    #[arg(long, default_value_t = 1)]
    partitions: i32,

    /// Outstanding-ack depth held open by the runner.
    #[arg(long, default_value_t = 1000)]
    in_flight: usize,

    /// Acknowledgement level: `none`, `leader`, or `all`.
    #[arg(long, default_value = "all")]
    acks: String,

    /// Producer linger, in milliseconds.
    #[arg(long, default_value_t = 5)]
    linger_ms: u64,

    /// Producer batch size, in bytes.
    #[arg(long, default_value_t = 16_384)]
    batch_bytes: usize,

    /// Keep the per-cell topics instead of deleting them after each cell.
    #[arg(long, default_value_t = false)]
    keep_topic: bool,

    /// Seed for the pseudo-random payload generator. Fixed default keeps
    /// runs reproducible.
    #[arg(long, default_value_t = 0x5EED_C0DE)]
    seed: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Debug-build numbers are meaningless — the whole point is to measure
    // optimized code. Warn loudly but continue, so the harness itself can
    // still be smoke-tested.
    if cfg!(debug_assertions) {
        eprintln!(
            "WARNING: this is a DEBUG build — throughput/latency numbers are meaningless. \
             Rebuild with `cargo run --release -p kafka-client-bench`."
        );
    }

    let acks = AcksLevel::parse(&args.acks)?;
    let crates: Vec<CrateId> = args
        .crates
        .iter()
        .map(|s| CrateId::parse(s))
        .collect::<anyhow::Result<_>>()?;
    anyhow::ensure!(!crates.is_empty(), "no crates selected");
    anyhow::ensure!(!args.record_sizes.is_empty(), "no record sizes given");
    anyhow::ensure!(args.in_flight >= 1, "--in-flight must be >= 1");
    let duration = args.duration.map(Duration::from_secs);

    // Broker connectivity pre-check: fail fast on a bad `--bootstrap`
    // rather than deep inside the first cell. This `Client` is reused for
    // all topic admin (each crate's producer opens its own connection).
    let (host, port) = producers::parse_host_port(&args.bootstrap)?;
    let admin_client = Client::connect(&[Config::new(host, port)], Security::Plaintext, Auth::None)
        .await
        .with_context(|| {
            format!(
                "could not connect to broker at '{}' — is it running, and is \
             KAFKA_BOOTSTRAP / --bootstrap correct?",
                args.bootstrap
            )
        })?;
    admin_client
        .refresh_metadata()
        .await
        .context("connected to broker but the initial metadata fetch failed")?;

    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    println!(
        "producer-bench: broker={} crates={} sizes={:?} {} warmup={} in-flight={} \
         acks={} linger={}ms batch-bytes={} seed={:#x}",
        args.bootstrap,
        args.crates.join(","),
        args.record_sizes,
        match duration {
            Some(d) => format!("duration={}s", d.as_secs()),
            None => format!("records={}", args.records),
        },
        args.warmup_records,
        args.in_flight,
        acks.label(),
        args.linger_ms,
        args.batch_bytes,
        args.seed,
    );

    let mut rng = fastrand::Rng::with_seed(args.seed);

    for &size in &args.record_sizes {
        // One payload buffer per size: pseudo-random bytes, so the
        // on-wire payload size ≈ the value-bytes count used for MB/s.
        let payload: Bytes = (0..size).map(|_| rng.u8(..)).collect();

        let mut results: Vec<(CrateId, anyhow::Result<workload::RunResult>)> = Vec::new();
        // Crates run strictly sequentially — never two against the broker
        // at once — so one crate's load can't skew another's numbers.
        for &crate_id in &crates {
            let unix_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let topic = format!(
                "{}-{}-{}-{}",
                args.topic_prefix,
                size,
                crate_id.label(),
                unix_nanos,
            );

            let result = run_cell(
                &admin_client,
                crate_id,
                &topic,
                size,
                &args,
                acks,
                duration,
                payload.clone(),
            )
            .await;
            results.push((crate_id, result));
        }

        print_result_table(
            size,
            &reference_workload(size, &args, acks, duration),
            &results,
        );
    }

    print_footer(&args, cpus);
    admin_client.close();
    Ok(())
}

/// Run one `(crate, size)` cell end to end: create topic → build + run the
/// producer → delete topic. Topic teardown still runs if the measured run
/// failed, so a bad cell doesn't leak topics onto a shared broker.
async fn run_cell(
    admin_client: &Client,
    crate_id: CrateId,
    topic: &str,
    size: usize,
    args: &Args,
    acks: AcksLevel,
    duration: Option<Duration>,
    payload: Bytes,
) -> anyhow::Result<workload::RunResult> {
    admin::create_topic(admin_client, topic, args.partitions)
        .await
        .with_context(|| format!("creating topic '{topic}'"))?;

    let workload = Workload {
        bootstrap: args.bootstrap.clone(),
        topic: topic.to_owned(),
        partitions: args.partitions,
        record_size: size,
        record_count: args.records,
        warmup_records: args.warmup_records,
        in_flight: args.in_flight,
        acks,
        linger: Duration::from_millis(args.linger_ms),
        batch_size_bytes: args.batch_bytes,
        duration,
    };

    let result = producers::run_bench(crate_id, &workload, payload).await;

    if !args.keep_topic {
        if let Err(e) = admin::delete_topic(admin_client, topic).await {
            eprintln!("WARNING: failed to delete topic '{topic}': {e}");
        }
    }

    result
}

/// A `Workload` carrying only the fields `print_result_table` reads for its
/// header line — the per-cell topic is irrelevant there.
fn reference_workload(
    size: usize,
    args: &Args,
    acks: AcksLevel,
    duration: Option<Duration>,
) -> Workload {
    Workload {
        bootstrap: args.bootstrap.clone(),
        topic: String::new(),
        partitions: args.partitions,
        record_size: size,
        record_count: args.records,
        warmup_records: args.warmup_records,
        in_flight: args.in_flight,
        acks,
        linger: Duration::from_millis(args.linger_ms),
        batch_size_bytes: args.batch_bytes,
        duration,
    }
}

/// Print the methodology footer: run environment plus every place a crate
/// could not be configured equivalently. Asymmetries are surfaced, not
/// hidden.
fn print_footer(args: &Args, cpus: usize) {
    println!("\n--- methodology ---");
    println!("broker:        {}", args.bootstrap);
    println!(
        "host CPUs:     {}",
        if cpus == 0 {
            "unknown".to_string()
        } else {
            cpus.to_string()
        },
    );
    println!(
        "build profile: {}",
        if cfg!(debug_assertions) {
            "debug (NUMBERS MEANINGLESS)"
        } else {
            "release"
        },
    );
    println!(
        "warmup:        {} records/cell, discarded",
        args.warmup_records
    );
    println!("topics:        one per (size, crate), wait_for_propagation=true");
    println!("clock:         starts before first measured send, stops after last ack + finalize()");
    println!("asymmetries:");
    println!(
        "  this:    background accumulator + linger/batching; uses a zeropool buffer pool \
         (warmup matters here specifically)."
    );
    println!(
        "  rdkafka: librdkafka copies each payload into its C-side queue before the send \
         returns — an inherent extra copy, noted not corrected."
    );
    println!(
        "  rskafka: BatchProducer is per-partition — produces only to partition 0 (no spread \
         with --partitions > 1); PartitionClient::produce exposes no acks knob (effectively \
         acks=all, so --acks none|leader is NOT honored by this column); Record holds Vec<u8>, \
         so the payload is copied per send."
    );
    println!(
        "  samsa:   its Producer API delivers acks as uncorrelated per-flush batches with no \
         per-record handle, so the bench runs it at max_batch_size=1 (one record per request) \
         to recover exact per-record latency. Client-side batching is therefore DISABLED — \
         samsa's throughput is unbatched/RTT-bound and NOT comparable to the batched columns; \
         batch_timeout_ms (linger) is inert; effective in-flight depth is ~1; produces only to \
         partition 0."
    );
}
