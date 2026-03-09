//! Single-thread comparison between `feldera-buffer-cache`'s sharded SIEVE
//! cache and `dbsp`'s current mutex-protected LRU buffer cache.
//!
//! `cargo bench -p dbsp --bench buffer_cache_compare -- --capacity-mib 8192`

use clap::Parser;
use dbsp::mimalloc::MiMalloc;
use dbsp::storage::buffer_cache::BufferCache;
use feldera_buffer_cache::SieveCache;
use std::sync::Arc;
use std::time::Instant;

#[path = "buffer_cache/common.rs"]
mod common;

use common::{
    BenchFile, CacheValue, DEFAULT_SINGLE_OPS_PER_RATIO, LruBenchEntry, MIB, Op, Prefill,
    build_prefill, build_trace, build_universe, parse_u8_csv,
};

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

#[derive(Parser, Debug, Clone)]
#[command(name = "buffer_cache_compare")]
#[command(about = "Single-thread throughput comparison: dbsp LRU vs 1-shard SIEVE")]
struct Args {
    /// Total cache capacity in MiB.
    #[arg(long, default_value_t = 256)]
    capacity_mib: usize,

    /// Key universe size.
    #[arg(long, default_value_t = 10_000_000)]
    key_space: usize,

    /// Number of operations per write ratio point.
    #[arg(long, default_value_t = DEFAULT_SINGLE_OPS_PER_RATIO)]
    ops_per_ratio: usize,

    /// Comma-separated write ratios in percent.
    #[arg(long, default_value = "1,10,20,30,40,50")]
    write_ratios: String,

    /// Zipf skew parameter `a` for key-access distribution.
    #[arg(long, default_value_t = 1.1)]
    zipf_a: f64,

    #[doc(hidden)]
    #[arg(long = "bench", hide = true)]
    __bench: bool,
}

#[derive(Default)]
struct RunStats {
    ops_per_sec: f64,
    hit_rate: f64,
    final_charge: usize,
}

fn run_sieve(prefill: &[Prefill], trace: &[Op], capacity_bytes: usize) -> RunStats {
    let cache = SieveCache::<u64, CacheValue>::with_shards(capacity_bytes, 1);
    for p in prefill {
        cache.insert(p.offset, CacheValue::new(p.charge), p.charge);
    }

    let mut reads = 0usize;
    let mut hits = 0usize;
    let start = Instant::now();

    for op in trace {
        if op.write {
            cache.insert(op.offset, CacheValue::new(op.charge), op.charge);
        } else {
            reads += 1;
            if cache.get(&op.offset).is_some() {
                hits += 1;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    RunStats {
        ops_per_sec: trace.len() as f64 / elapsed,
        hit_rate: if reads == 0 {
            0.0
        } else {
            hits as f64 / reads as f64
        },
        final_charge: cache.total_charge(),
    }
}

fn run_lru(prefill: &[Prefill], trace: &[Op], capacity_bytes: usize) -> RunStats {
    let cache = BufferCache::new(capacity_bytes);
    let file = BenchFile::new();
    for p in prefill {
        cache.insert(file.id(), p.offset, Arc::new(LruBenchEntry::new(p.charge)));
    }

    let mut reads = 0usize;
    let mut hits = 0usize;
    let start = Instant::now();

    for op in trace {
        if op.write {
            cache.insert(
                file.id(),
                op.offset,
                Arc::new(LruBenchEntry::new(op.charge)),
            );
        } else {
            reads += 1;
            if cache
                .get(&file, file.location(op.offset, op.charge))
                .is_some()
            {
                hits += 1;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    RunStats {
        ops_per_sec: trace.len() as f64 / elapsed,
        hit_rate: if reads == 0 {
            0.0
        } else {
            hits as f64 / reads as f64
        },
        final_charge: cache.occupancy().0,
    }
}

fn main() {
    let args = Args::parse();
    let capacity_bytes = args.capacity_mib.saturating_mul(MIB);
    let write_ratios = parse_u8_csv(&args.write_ratios);

    assert!(args.key_space > 0, "key-space must be > 0");
    assert!(!write_ratios.is_empty(), "write-ratios cannot be empty");
    assert!(args.ops_per_ratio > 0, "ops-per-ratio must be > 0");
    assert!(args.zipf_a >= 0.0, "zipf-a must be >= 0");
    for &ratio in &write_ratios {
        assert!(ratio <= 100, "write ratio must be in [0,100]");
    }

    let charge_by_key = build_universe(args.key_space);
    let prefill = build_prefill(&charge_by_key, capacity_bytes);

    println!(
        "single-thread benchmark, capacity={} MiB, key_space={}, ops_per_ratio={}, zipf_a={}\n",
        capacity_bytes / MIB,
        args.key_space,
        args.ops_per_ratio,
        args.zipf_a
    );
    println!(
        "os,zipf_a,write_ratio,sieve_ops_s,lru_ops_s,sieve_hit,lru_hit,sieve_charge,lru_charge,sieve_vs_lru_speedup"
    );

    for &ratio in &write_ratios {
        let trace = build_trace(ratio, args.ops_per_ratio, &charge_by_key, args.zipf_a);
        let sieve = run_sieve(&prefill, &trace, capacity_bytes);
        let lru = run_lru(&prefill, &trace, capacity_bytes);
        let speedup = if lru.ops_per_sec > 0.0 {
            sieve.ops_per_sec / lru.ops_per_sec
        } else {
            0.0
        };

        println!(
            "{},{:.3},{ratio},{:.0},{:.0},{:.3},{:.3},{},{},{:.3}",
            std::env::consts::OS,
            args.zipf_a,
            sieve.ops_per_sec,
            lru.ops_per_sec,
            sieve.hit_rate,
            lru.hit_rate,
            sieve.final_charge,
            lru.final_charge,
            speedup
        );
    }
}
