#![allow(dead_code)]

use dbsp::storage::buffer_cache::{CacheEntry, FBuf};
use feldera_storage::block::BlockLocation;
use feldera_storage::error::StorageError;
use feldera_storage::file::FileId;
use feldera_storage::{FileCommitter, FileReader, FileRw, StoragePath};
use rand::rngs::ThreadRng;
use rand::{Rng, thread_rng};
use rand_distr::{Distribution, Zipf};
use std::sync::Arc;

pub const MIB: usize = 1024 * 1024;
pub const DEFAULT_SINGLE_OPS_PER_RATIO: usize = 50_000_000;
pub const DEFAULT_THROUGHPUT_OPS_PER_RATIO: usize = 20_000_000;

const BLOCK_ALIGNMENT: u64 = 512;
const SIZE_CLASSES: [usize; 6] = [
    4 * 1024,
    8 * 1024,
    16 * 1024,
    32 * 1024,
    64 * 1024,
    128 * 1024,
];

#[derive(Clone, Copy)]
pub struct Op {
    pub offset: u64,
    pub charge: usize,
    pub write: bool,
}

#[derive(Clone, Copy)]
pub struct Prefill {
    pub offset: u64,
    pub charge: usize,
}

#[derive(Debug, Clone)]
pub struct BenchFile {
    id: FileId,
    path: StoragePath,
}

impl BenchFile {
    pub fn new() -> Self {
        Self {
            id: FileId::new(),
            path: StoragePath::default(),
        }
    }

    pub fn id(&self) -> FileId {
        self.id
    }

    pub fn location(&self, offset: u64, size: usize) -> BlockLocation {
        BlockLocation { offset, size }
    }
}

impl FileRw for BenchFile {
    fn file_id(&self) -> FileId {
        self.id
    }

    fn path(&self) -> &StoragePath {
        &self.path
    }
}

impl FileCommitter for BenchFile {
    fn commit(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

impl FileReader for BenchFile {
    fn mark_for_checkpoint(&self) {}

    fn read_block(&self, _location: BlockLocation) -> Result<Arc<FBuf>, StorageError> {
        unreachable!("buffer-cache benchmarks never read from the backing file")
    }

    fn get_size(&self) -> Result<u64, StorageError> {
        unreachable!("buffer-cache benchmarks never read the backing file size")
    }
}

pub struct CacheValue(Vec<u8>);

impl CacheValue {
    pub fn new(charge: usize) -> Self {
        Self(vec![0u8; charge])
    }
}

pub struct LruBenchEntry {
    value: CacheValue,
}

impl LruBenchEntry {
    pub fn new(charge: usize) -> Self {
        Self {
            value: CacheValue::new(charge),
        }
    }
}

impl CacheEntry for LruBenchEntry {
    fn cost(&self) -> usize {
        self.value.0.len()
    }
}

fn sample_zipf_class(rng: &mut ThreadRng, zipf: &Zipf<f64>) -> usize {
    let rank = zipf.sample(rng) as usize;
    rank.saturating_sub(1).min(SIZE_CLASSES.len() - 1)
}

fn key_offset(key_idx: usize) -> u64 {
    key_idx as u64 * BLOCK_ALIGNMENT
}

pub fn parse_u8_csv(s: &str) -> Vec<u8> {
    let mut out: Vec<u8> = s
        .split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse::<u8>().expect("invalid u8 in CSV"))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

pub fn parse_usize_csv(s: &str) -> Vec<usize> {
    let mut out: Vec<usize> = s
        .split(',')
        .filter(|x| !x.is_empty())
        .map(|x| x.trim().parse::<usize>().expect("invalid usize in CSV"))
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

pub fn build_universe(key_space: usize) -> Vec<usize> {
    let zipf = Zipf::new(SIZE_CLASSES.len() as u64, 1.2_f64).expect("valid zipf params");
    let mut rng = thread_rng();
    let mut charge_by_key = vec![0usize; key_space];

    for charge in &mut charge_by_key {
        let class = sample_zipf_class(&mut rng, &zipf);
        *charge = SIZE_CLASSES[class];
    }

    charge_by_key
}

pub fn build_prefill(charge_by_key: &[usize], capacity_bytes: usize) -> Vec<Prefill> {
    let mut prefill = Vec::new();
    let mut total = 0usize;
    for (key_idx, &charge) in charge_by_key.iter().enumerate() {
        prefill.push(Prefill {
            offset: key_offset(key_idx),
            charge,
        });
        total = total.saturating_add(charge);
        if total >= capacity_bytes {
            break;
        }
    }
    prefill
}

pub fn build_trace(write_ratio: u8, ops: usize, charge_by_key: &[usize], zipf_a: f64) -> Vec<Op> {
    let key_space = charge_by_key.len();
    let zipf = Zipf::new(key_space as u64, zipf_a).expect("valid zipf params");
    let mut rng = thread_rng();
    let mut trace = Vec::with_capacity(ops);

    for _ in 0..ops {
        let key_idx = (zipf.sample(&mut rng) as usize)
            .saturating_sub(1)
            .min(key_space.saturating_sub(1));
        trace.push(Op {
            offset: key_offset(key_idx),
            charge: charge_by_key[key_idx],
            write: rng.gen_ratio(write_ratio as u32, 100),
        });
    }

    trace
}

pub fn build_thread_traces(
    write_ratio: u8,
    ops_per_thread: usize,
    charge_by_key: &[usize],
    zipf_a: f64,
    threads: usize,
) -> Vec<Vec<Op>> {
    let key_space = charge_by_key.len();
    let zipf = Zipf::new(key_space as u64, zipf_a).expect("valid zipf params");

    (0..threads)
        .map(|_| {
            let mut rng = thread_rng();
            let mut trace = Vec::with_capacity(ops_per_thread);
            for _ in 0..ops_per_thread {
                let key_idx = (zipf.sample(&mut rng) as usize)
                    .saturating_sub(1)
                    .min(key_space.saturating_sub(1));
                trace.push(Op {
                    offset: key_offset(key_idx),
                    charge: charge_by_key[key_idx],
                    write: rng.gen_ratio(write_ratio as u32, 100),
                });
            }
            trace
        })
        .collect()
}
