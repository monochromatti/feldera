//! Multi-thread throughput comparison between `feldera-buffer-cache`'s sharded
//! SIEVE cache and `dbsp`'s current mutex-protected LRU buffer cache.
//!
//! `cargo bench -p dbsp --bench buffer_cache_throughput -- --write-ratios 0 --capacity-mib 1024 --strategy sieve,lru --max-duration 30`

use clap::{Parser, ValueEnum};
use dbsp::mimalloc::MiMalloc;
use dbsp::storage::buffer_cache::BufferCache;
use feldera_buffer_cache::SieveCache;
use std::sync::{Arc, Barrier, OnceLock};
use std::time::{Duration, Instant};

#[path = "buffer_cache/common.rs"]
mod common;

use common::{
    BenchFile, CacheValue, DEFAULT_THROUGHPUT_OPS_PER_RATIO, LruBenchEntry, MIB, Op, Prefill,
    build_prefill, build_thread_traces, build_universe, parse_u8_csv, parse_usize_csv,
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

#[derive(Parser, Debug, Clone)]
#[command(name = "buffer_cache_throughput")]
#[command(about = "Multi-thread throughput scaling: dbsp LRU(mutex) vs sharded SIEVE")]
struct Args {
    /// Total cache capacity in MiB.
    #[arg(long, default_value_t = 256)]
    capacity_mib: usize,

    /// Key universe size.
    #[arg(long, default_value_t = 10_000_000)]
    key_space: usize,

    /// Operations per thread per write-ratio point.
    #[arg(long, default_value_t = DEFAULT_THROUGHPUT_OPS_PER_RATIO)]
    ops_per_ratio: usize,

    /// Comma-separated write ratios in percent.
    #[arg(long, default_value = "1,10,20,30,40,50")]
    write_ratios: String,

    /// Zipf skew parameter `a` for key-access distribution.
    #[arg(long, default_value_t = 1.1)]
    zipf_a: f64,

    /// Comma-separated thread counts.
    #[arg(long, default_value = "1,2,4,8")]
    threads: String,

    /// Comma-separated strategies to run: sieve,lru.
    #[arg(long, value_delimiter = ',', default_value = "sieve,lru")]
    strategy: Vec<Strategy>,

    /// Optional max duration (seconds) per run. If reached, stop early and report partial stats.
    #[arg(long)]
    max_duration: Option<f64>,

    #[doc(hidden)]
    #[arg(long = "bench", hide = true)]
    __bench: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Strategy {
    Sieve,
    Lru,
}

#[derive(Default, Clone, Copy)]
struct RunStats {
    ops_per_sec: f64,
    total_reads: f64,
    total_writes: f64,
    hit_rate: f64,
}

fn run_sieve_mt(
    prefill: &[Prefill],
    traces: &[Vec<Op>],
    capacity_bytes: usize,
    max_duration: Option<Duration>,
) -> RunStats {
    let cache = Arc::new(SieveCache::<u64, CacheValue>::new(capacity_bytes));
    let workers = traces.len();
    let prefill_done = Barrier::new(workers + 1);
    let ready = Barrier::new(workers + 1);
    let start_gate = Barrier::new(workers + 1);
    let run_start: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);

        for (worker_idx, trace) in traces.iter().enumerate() {
            let cache = cache.clone();
            let prefill_done = &prefill_done;
            let ready = &ready;
            let start_gate = &start_gate;
            let run_start = run_start.clone();
            let prefill_start = worker_idx * prefill.len() / workers;
            let prefill_end = (worker_idx + 1) * prefill.len() / workers;

            handles.push(scope.spawn(move || {
                for p in &prefill[prefill_start..prefill_end] {
                    cache.insert(p.offset, CacheValue::new(p.charge), p.charge);
                }

                prefill_done.wait();
                ready.wait();
                start_gate.wait();
                let run_start = *run_start.get().expect("run start must be initialized");

                let mut ops = 0usize;
                let mut writes = 0usize;
                let mut reads = 0usize;
                let mut hits = 0usize;
                for op in trace {
                    if let Some(limit) = max_duration
                        && run_start.elapsed() >= limit
                    {
                        break;
                    }
                    ops += 1;
                    if op.write {
                        writes += 1;
                        cache.insert(op.offset, CacheValue::new(op.charge), op.charge);
                    } else {
                        reads += 1;
                        if cache.get(&op.offset).is_some() {
                            hits += 1;
                        }
                    }
                }
                (ops, reads, writes, hits)
            }));
        }

        prefill_done.wait();
        ready.wait();
        let start = Instant::now();
        run_start
            .set(start)
            .expect("run start should only be set once");
        start_gate.wait();

        let mut total_ops = 0usize;
        let mut total_reads = 0usize;
        let mut total_writes = 0usize;
        let mut total_hits = 0usize;
        for handle in handles {
            let (ops, reads, writes, hits) = handle.join().expect("sieve thread panicked");
            total_ops += ops;
            total_reads += reads;
            total_writes += writes;
            total_hits += hits;
        }

        let elapsed = start.elapsed().as_secs_f64();
        RunStats {
            ops_per_sec: total_ops as f64 / elapsed,
            total_reads: total_reads as f64,
            total_writes: total_writes as f64,
            hit_rate: if total_reads == 0 {
                0.0
            } else {
                total_hits as f64 / total_reads as f64
            },
        }
    })
}

fn run_lru_mt(
    prefill: &[Prefill],
    traces: &[Vec<Op>],
    capacity_bytes: usize,
    max_duration: Option<Duration>,
) -> RunStats {
    let cache = Arc::new(BufferCache::new(capacity_bytes));
    let file = Arc::new(BenchFile::new());

    for p in prefill {
        cache.insert(file.id(), p.offset, Arc::new(LruBenchEntry::new(p.charge)));
    }

    let ready = Arc::new(Barrier::new(traces.len() + 1));
    let start_gate = Arc::new(Barrier::new(traces.len() + 1));
    let run_start: Arc<OnceLock<Instant>> = Arc::new(OnceLock::new());
    let mut handles = Vec::with_capacity(traces.len());

    for trace in traces {
        let cache = cache.clone();
        let file = file.clone();
        let trace = trace.clone();
        let ready = ready.clone();
        let start_gate = start_gate.clone();
        let run_start = run_start.clone();

        handles.push(std::thread::spawn(move || {
            ready.wait();
            start_gate.wait();
            let run_start = *run_start.get().expect("run start must be initialized");
            let mut ops = 0usize;
            let mut writes = 0usize;
            let mut reads = 0usize;
            let mut hits = 0usize;

            for op in &trace {
                if let Some(limit) = max_duration
                    && run_start.elapsed() >= limit
                {
                    break;
                }
                ops += 1;
                if op.write {
                    writes += 1;
                    cache.insert(
                        file.id(),
                        op.offset,
                        Arc::new(LruBenchEntry::new(op.charge)),
                    );
                } else {
                    reads += 1;
                    if cache
                        .get(file.as_ref(), file.location(op.offset, op.charge))
                        .is_some()
                    {
                        hits += 1;
                    }
                }
            }

            (ops, reads, writes, hits)
        }));
    }

    ready.wait();
    let start = Instant::now();
    run_start
        .set(start)
        .expect("run start should only be set once");
    start_gate.wait();

    let mut total_ops = 0usize;
    let mut total_reads = 0usize;
    let mut total_writes = 0usize;
    let mut total_hits = 0usize;
    for handle in handles {
        let (ops, reads, writes, hits) = handle.join().expect("lru thread panicked");
        total_ops += ops;
        total_reads += reads;
        total_writes += writes;
        total_hits += hits;
    }

    let elapsed = start.elapsed().as_secs_f64();
    RunStats {
        ops_per_sec: total_ops as f64 / elapsed,
        total_reads: total_reads as f64,
        total_writes: total_writes as f64,
        hit_rate: if total_reads == 0 {
            0.0
        } else {
            total_hits as f64 / total_reads as f64
        },
    }
}

fn main() {
    let args = Args::parse();
    let capacity_bytes = args.capacity_mib.saturating_mul(MIB);
    let write_ratios = parse_u8_csv(&args.write_ratios);
    let thread_counts = parse_usize_csv(&args.threads);
    let run_sieve = args.strategy.contains(&Strategy::Sieve);
    let run_lru = args.strategy.contains(&Strategy::Lru);

    assert!(args.key_space > 0, "key-space must be > 0");
    assert!(!write_ratios.is_empty(), "write-ratios cannot be empty");
    assert!(!thread_counts.is_empty(), "threads cannot be empty");
    assert!(
        run_sieve || run_lru,
        "strategy cannot be empty; choose one or more of sieve,lru"
    );
    assert!(args.ops_per_ratio > 0, "ops-per-ratio must be > 0");
    assert!(args.zipf_a >= 0.0, "zipf-a must be >= 0");
    if let Some(max_duration) = args.max_duration {
        assert!(
            max_duration.is_finite() && max_duration > 0.0,
            "max-duration must be finite and > 0"
        );
    }
    for &ratio in &write_ratios {
        assert!(ratio <= 100, "write ratio must be in [0,100]");
    }
    for &threads in &thread_counts {
        assert!(threads > 0, "thread count must be > 0");
    }

    let charge_by_key = build_universe(args.key_space);
    let prefill = build_prefill(&charge_by_key, capacity_bytes);
    let max_duration = args.max_duration.map(Duration::from_secs_f64);
    let max_duration_str = args
        .max_duration
        .map_or_else(|| "none".to_string(), |value| format!("{value:.3}"));

    println!(
        "capacity_mib={}, key_space={}, base_ops_per_ratio={}, zipf_a={}, max_duration_s={}\n",
        args.capacity_mib, args.key_space, args.ops_per_ratio, args.zipf_a, max_duration_str
    );
    println!(
        "os,zipf_a,write_ratio,threads,ops_per_thread,total_ops,total_reads,total_writes,sieve_ops_s,lru_ops_s,sieve_hit,lru_hit,sieve_scaling,lru_scaling,sieve_vs_lru"
    );

    for &ratio in &write_ratios {
        let mut sieve_base = None::<f64>;
        let mut lru_base = None::<f64>;

        for &threads in &thread_counts {
            let ops_per_thread = ((args.ops_per_ratio as f64) * (threads as f64) * 0.5)
                .round()
                .max(1.0) as usize;
            let total_ops = ops_per_thread.saturating_mul(threads);
            let traces =
                build_thread_traces(ratio, ops_per_thread, &charge_by_key, args.zipf_a, threads);

            let sieve = if run_sieve {
                Some(run_sieve_mt(
                    &prefill,
                    &traces,
                    capacity_bytes,
                    max_duration,
                ))
            } else {
                None
            };
            let lru = if run_lru {
                Some(run_lru_mt(&prefill, &traces, capacity_bytes, max_duration))
            } else {
                None
            };

            if max_duration.is_none()
                && let (Some(sieve), Some(lru)) = (sieve, lru)
            {
                assert!(
                    sieve.total_reads == lru.total_reads && sieve.total_writes == lru.total_writes,
                    "read/write totals diverged between sieve and lru at write_ratio={}, threads={}: sieve(reads={}, writes={}) lru(reads={}, writes={})",
                    ratio,
                    threads,
                    sieve.total_reads,
                    sieve.total_writes,
                    lru.total_reads,
                    lru.total_writes
                );
            }

            let sieve_base_value = sieve.map(|stats| *sieve_base.get_or_insert(stats.ops_per_sec));
            let lru_base_value = lru.map(|stats| *lru_base.get_or_insert(stats.ops_per_sec));

            let sieve_scaling = match (sieve, sieve_base_value) {
                (Some(stats), Some(base)) if base > 0.0 => stats.ops_per_sec / base,
                (Some(_), _) => 0.0,
                (None, _) => f64::NAN,
            };
            let lru_scaling = match (lru, lru_base_value) {
                (Some(stats), Some(base)) if base > 0.0 => stats.ops_per_sec / base,
                (Some(_), _) => 0.0,
                (None, _) => f64::NAN,
            };
            let sieve_vs_lru = match (sieve, lru) {
                (Some(sieve), Some(lru)) if lru.ops_per_sec > 0.0 => {
                    sieve.ops_per_sec / lru.ops_per_sec
                }
                (Some(_), Some(_)) => 0.0,
                _ => f64::NAN,
            };
            let total_reads = match (sieve, lru) {
                (Some(stats), _) => stats.total_reads,
                (None, Some(stats)) => stats.total_reads,
                (None, None) => f64::NAN,
            };
            let total_writes = match (sieve, lru) {
                (Some(stats), _) => stats.total_writes,
                (None, Some(stats)) => stats.total_writes,
                (None, None) => f64::NAN,
            };

            println!(
                "{},{:.3},{},{},{},{},{:.0},{:.0},{:.0},{:.0},{:.3},{:.3},{:.3},{:.3},{:.3}",
                std::env::consts::OS,
                args.zipf_a,
                ratio,
                threads,
                ops_per_thread,
                total_ops,
                total_reads,
                total_writes,
                sieve.map_or(f64::NAN, |stats| stats.ops_per_sec),
                lru.map_or(f64::NAN, |stats| stats.ops_per_sec),
                sieve.map_or(f64::NAN, |stats| stats.hit_rate),
                lru.map_or(f64::NAN, |stats| stats.hit_rate),
                sieve_scaling,
                lru_scaling,
                sieve_vs_lru
            );
        }
    }
}
