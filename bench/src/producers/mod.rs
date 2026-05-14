//! The four `BenchProducer` impls and the dispatch that builds + runs one.

use bytes::Bytes;

use crate::bench_producer;
use crate::workload::{RunResult, Workload};

pub mod rdkafka;
pub mod rskafka;
pub mod samsa;
pub mod this_client;

/// Which crate a cell exercises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CrateId {
    /// This library (`kafka-client`).
    This,
    Rdkafka,
    Rskafka,
    Samsa,
}

impl CrateId {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.trim().to_lowercase().as_str() {
            "this" => Ok(Self::This),
            "rdkafka" => Ok(Self::Rdkafka),
            "rskafka" => Ok(Self::Rskafka),
            "samsa" => Ok(Self::Samsa),
            other => anyhow::bail!("unknown crate '{other}' (expected this|rdkafka|rskafka|samsa)"),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::This => "this",
            Self::Rdkafka => "rdkafka",
            Self::Rskafka => "rskafka",
            Self::Samsa => "samsa",
        }
    }
}

/// Build the concrete producer for `crate_id` and run one full cell
/// through the shared runner. Kept as a `match` (rather than `dyn`) so the
/// runner stays monomorphised per crate.
pub async fn run_bench(
    crate_id: CrateId,
    workload: &Workload,
    payload: Bytes,
) -> anyhow::Result<RunResult> {
    println!("Running bench {:?}", crate_id);
    match crate_id {
        CrateId::This => {
            let p = this_client::ThisProducer::connect(workload).await?;
            bench_producer::run(p, workload, payload).await
        }
        CrateId::Rdkafka => {
            let p = rdkafka::RdkafkaProducer::connect(workload)?;
            bench_producer::run(p, workload, payload).await
        }
        CrateId::Rskafka => {
            let p = rskafka::RskafkaProducer::connect(workload).await?;
            bench_producer::run(p, workload, payload).await
        }
        CrateId::Samsa => {
            let p = samsa::SamsaProducer::connect(workload).await?;
            bench_producer::run(p, workload, payload).await
        }
    }
}

/// Split a `host:port` bootstrap string. A missing port defaults to 9092.
pub(crate) fn parse_host_port(bootstrap: &str) -> anyhow::Result<(String, u16)> {
    match bootstrap.rsplit_once(':') {
        Some((host, port)) => {
            let port: u16 = port
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid port in bootstrap '{bootstrap}'"))?;
            anyhow::ensure!(!host.is_empty(), "empty host in bootstrap '{bootstrap}'");
            Ok((host.to_owned(), port))
        }
        None => {
            anyhow::ensure!(!bootstrap.is_empty(), "empty bootstrap address");
            Ok((bootstrap.to_owned(), 9092))
        }
    }
}
