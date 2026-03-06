//! Buffer-cache implementations used by DBSP storage.
//!
//! `BufferCache` keeps the existing mutex-protected LRU implementation around
//! and can also wrap the sharded SIEVE cache from `feldera-buffer-cache`.
use std::any::Any;
use std::fmt::{Debug, Display};
use std::ops::{Add, AddAssign};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{collections::BTreeMap, ops::Range};

use enum_map::{Enum, EnumMap};
use feldera_buffer_cache::SieveCache;
use serde::{Deserialize, Serialize};
use size_of::SizeOf;

use crate::circuit::metadata::{
    CACHE_BACKGROUND_HIT_RATE_PERCENT, CACHE_BACKGROUND_HITS, CACHE_BACKGROUND_MISSES,
    CACHE_FOREGROUND_HIT_RATE_PERCENT, CACHE_FOREGROUND_HITS, CACHE_FOREGROUND_MISSES, MetaItem,
    MetricId, MetricReading, OperatorMeta,
};
use crate::circuit::runtime::ThreadType;
use crate::storage::backend::{BlockLocation, FileId, FileReader};

/// A key for the block cache.
///
/// The block size could be part of the key, but we'll never read a given offset
/// with more than one size so it's also not necessary.
///
/// It's important that the sort order is by `fd` first and `offset` second, so
/// that [`CacheKey::fd_range`] can work.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct CacheKey {
    /// File being cached.
    file_id: FileId,

    /// Offset in file.
    offset: u64,
}

impl CacheKey {
    fn new(file_id: FileId, offset: u64) -> Self {
        Self { file_id, offset }
    }

    /// Returns a range that would contain all of the blocks for the specified
    /// `fd`.
    fn file_range(file_id: FileId) -> Range<CacheKey> {
        Self { file_id, offset: 0 }..Self {
            file_id: file_id.after(),
            offset: 0,
        }
    }
}

/// Selects which eviction strategy backs [`BufferCache`].
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BufferCacheStrategy {
    /// Use the sharded SIEVE cache from `feldera-buffer-cache`.
    #[default]
    Sieve,

    /// Use DBSP's existing mutex-protected LRU implementation.
    Lru,
}

/// A value in the block cache.
struct CacheValue {
    /// Cached interpretation of `block`.
    aux: Arc<dyn CacheEntry>,

    /// Serial number for LRU purposes.  Blocks with higher serial numbers have
    /// been used more recently.
    serial: u64,
}

pub trait CacheEntry: Any + Send + Sync {
    fn cost(&self) -> usize;
}

impl dyn CacheEntry {
    pub fn downcast<T>(self: Arc<Self>) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        (self as Arc<dyn Any + Send + Sync>).downcast().ok()
    }
}

struct CacheInner {
    /// Cache contents.
    cache: BTreeMap<CacheKey, CacheValue>,

    /// Map from LRU serial number to cache key.  The element with the smallest
    /// serial number was least recently used.
    lru: BTreeMap<u64, CacheKey>,

    /// Serial number to use the next time we touch a block.
    next_serial: u64,

    /// Sum over `cache[*].block.cost()`.
    cur_cost: usize,

    /// Maximum `size`, in bytes.
    max_cost: usize,
}

impl CacheInner {
    fn new(max_cost: usize) -> Self {
        Self {
            cache: BTreeMap::new(),
            lru: BTreeMap::new(),
            next_serial: 0,
            cur_cost: 0,
            max_cost,
        }
    }

    #[allow(dead_code)]
    fn check_invariants(&self) {
        assert_eq!(self.cache.len(), self.lru.len());
        let mut cost = 0;
        for (key, value) in self.cache.iter() {
            assert_eq!(self.lru.get(&value.serial), Some(key));
            cost += value.aux.cost();
        }
        for (serial, key) in self.lru.iter() {
            assert_eq!(self.cache.get(key).unwrap().serial, *serial);
        }
        assert_eq!(cost, self.cur_cost);
    }

    fn debug_check_invariants(&self) {
        #[cfg(debug_assertions)]
        self.check_invariants()
    }

    fn delete_file(&mut self, file_id: FileId) {
        let offsets: Vec<_> = self
            .cache
            .range(CacheKey::file_range(file_id))
            .map(|(k, v)| (k.offset, v.serial))
            .collect();
        for (offset, serial) in offsets {
            self.lru.remove(&serial).unwrap();
            self.cur_cost -= self
                .cache
                .remove(&CacheKey::new(file_id, offset))
                .unwrap()
                .aux
                .cost();
        }
        self.debug_check_invariants();
    }

    fn get(&mut self, key: CacheKey) -> Option<Arc<dyn CacheEntry>> {
        if let Some(value) = self.cache.get_mut(&key) {
            self.lru.remove(&value.serial);
            value.serial = self.next_serial;
            self.lru.insert(value.serial, key);
            self.next_serial += 1;
            Some(value.aux.clone())
        } else {
            None
        }
    }

    fn evict_to(&mut self, max_size: usize) {
        while self.cur_cost > max_size {
            let (_serial, key) = self.lru.pop_first().unwrap();
            let value = self.cache.remove(&key).unwrap();
            self.cur_cost -= value.aux.cost();
        }
        self.debug_check_invariants();
    }

    fn insert(&mut self, key: CacheKey, aux: Arc<dyn CacheEntry>) {
        let cost = aux.cost();
        self.evict_to(self.max_cost.saturating_sub(cost));
        if let Some(old_value) = self.cache.insert(
            key,
            CacheValue {
                aux,
                serial: self.next_serial,
            },
        ) {
            self.lru.remove(&old_value.serial);
            self.cur_cost -= old_value.aux.cost();
        }
        self.lru.insert(self.next_serial, key);
        self.cur_cost += cost;
        self.next_serial += 1;
        self.debug_check_invariants();
    }
}

/// A cache on top of a storage [backend](crate::storage::backend).
pub struct BufferCache {
    inner: BufferCacheInner,
}

enum BufferCacheInner {
    Lru(Mutex<CacheInner>),
    Sieve(SieveCache<CacheKey, Arc<dyn CacheEntry>>),
}

impl Debug for BufferCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferCache")
            .field("strategy", &self.strategy())
            .finish()
    }
}

impl BufferCache {
    /// Creates a new cache using the default [`BufferCacheStrategy`].
    ///
    /// `max_cost` limits the size of the cache in terms of [CacheEntry::cost].
    pub fn new(max_cost: usize) -> Self {
        Self::new_with_strategy(max_cost, BufferCacheStrategy::default(), None)
    }

    /// Creates a new cache with an explicit strategy.
    ///
    /// `max_buckets` only applies to [`BufferCacheStrategy::Sieve`]. The SIEVE
    /// implementation requires a power-of-two shard count, so values are rounded
    /// up to the next power of two.
    pub fn new_with_strategy(
        max_cost: usize,
        strategy: BufferCacheStrategy,
        max_buckets: Option<usize>,
    ) -> Self {
        let inner = match strategy {
            BufferCacheStrategy::Lru => {
                BufferCacheInner::Lru(Mutex::new(CacheInner::new(max_cost)))
            }
            BufferCacheStrategy::Sieve => {
                let shards = normalize_sieve_shards(max_buckets);
                BufferCacheInner::Sieve(SieveCache::with_shards(max_cost, shards))
            }
        };
        Self { inner }
    }

    /// Creates a new cache backed by DBSP's LRU implementation.
    pub fn new_lru(max_cost: usize) -> Self {
        Self::new_with_strategy(max_cost, BufferCacheStrategy::Lru, None)
    }

    /// Creates a new cache backed by the sharded SIEVE implementation.
    pub fn new_sieve(max_cost: usize, max_buckets: Option<usize>) -> Self {
        Self::new_with_strategy(max_cost, BufferCacheStrategy::Sieve, max_buckets)
    }

    /// Returns the strategy backing this cache.
    pub fn strategy(&self) -> BufferCacheStrategy {
        match &self.inner {
            BufferCacheInner::Lru(_) => BufferCacheStrategy::Lru,
            BufferCacheInner::Sieve(_) => BufferCacheStrategy::Sieve,
        }
    }

    pub fn get(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
    ) -> Option<Arc<dyn CacheEntry>> {
        let key = CacheKey::new(file.file_id(), location.offset);
        match &self.inner {
            BufferCacheInner::Lru(inner) => inner.lock().unwrap().get(key),
            BufferCacheInner::Sieve(inner) => {
                inner.get(&key).map(|entry| Arc::clone(entry.as_ref()))
            }
        }
    }

    pub fn insert(&self, file_id: FileId, offset: u64, aux: Arc<dyn CacheEntry>) {
        let key = CacheKey::new(file_id, offset);
        match &self.inner {
            BufferCacheInner::Lru(inner) => inner.lock().unwrap().insert(key, aux),
            BufferCacheInner::Sieve(inner) => {
                let cost = aux.cost();
                let _ = inner.insert(key, aux, cost);
            }
        }
    }

    pub fn evict(&self, file: &dyn FileReader) {
        match &self.inner {
            BufferCacheInner::Lru(inner) => inner.lock().unwrap().delete_file(file.file_id()),
            BufferCacheInner::Sieve(inner) => {
                inner.remove_if(|key| key.file_id == file.file_id());
            }
        }
    }

    /// Returns `(cur_cost, max_cost)`, reporting the amount of the cache that
    /// is currently used and the maximum value, both denominated in terms of
    /// [CacheEntry::cost] for `CacheEntry`.
    pub fn occupancy(&self) -> (usize, usize) {
        match &self.inner {
            BufferCacheInner::Lru(inner) => {
                let inner = inner.lock().unwrap();
                (inner.cur_cost, inner.max_cost)
            }
            BufferCacheInner::Sieve(inner) => (inner.total_charge(), inner.total_capacity()),
        }
    }
}

fn normalize_sieve_shards(max_buckets: Option<usize>) -> usize {
    match max_buckets {
        None => SieveCache::<CacheKey, Arc<dyn CacheEntry>>::DEFAULT_SHARDS,
        Some(0) => SieveCache::<CacheKey, Arc<dyn CacheEntry>>::DEFAULT_SHARDS,
        Some(buckets) if buckets.is_power_of_two() => buckets,
        Some(buckets) => buckets
            .checked_next_power_of_two()
            .unwrap_or(SieveCache::<CacheKey, Arc<dyn CacheEntry>>::DEFAULT_SHARDS),
    }
}

/// Cache statistics that can be accessed atomically for multithread updates.
#[derive(Debug, Default, SizeOf)]
#[size_of(skip_all)]
pub struct AtomicCacheStats(EnumMap<ThreadType, EnumMap<CacheAccess, AtomicCacheCounts>>);

impl AtomicCacheStats {
    /// Records that `location` was access in the cache with effect `access`.
    pub fn record(&self, access: CacheAccess, duration: Duration, location: BlockLocation) {
        let Some(thread_type) = ThreadType::current() else {
            // TODO: record stats for aux threads.
            return;
        };
        self.0[thread_type][access].record(duration, location);
    }

    /// Reads out the statistics for processing.
    pub fn read(&self) -> CacheStats {
        CacheStats(EnumMap::from_fn(|thread_type| {
            EnumMap::from_fn(|access| self.0[thread_type][access].read())
        }))
    }
}

/// Whether a cache access was a hit or a miss.
#[derive(Copy, Clone, Debug, Enum)]
pub enum CacheAccess {
    /// Cache hit.
    Hit,

    /// Cache miss.
    Miss,
}

fn cache_metric(thread_type: ThreadType, access: CacheAccess) -> MetricId {
    match (thread_type, access) {
        (ThreadType::Foreground, CacheAccess::Hit) => CACHE_FOREGROUND_HITS,
        (ThreadType::Foreground, CacheAccess::Miss) => CACHE_FOREGROUND_MISSES,
        (ThreadType::Background, CacheAccess::Hit) => CACHE_BACKGROUND_HITS,
        (ThreadType::Background, CacheAccess::Miss) => CACHE_BACKGROUND_MISSES,
        (ThreadType::MergerTokio, CacheAccess::Hit) => CACHE_BACKGROUND_HITS,
        (ThreadType::MergerTokio, CacheAccess::Miss) => CACHE_BACKGROUND_MISSES,
    }
}

fn cache_hit_rate_metric(thread_type: ThreadType) -> MetricId {
    match thread_type {
        ThreadType::Foreground => CACHE_FOREGROUND_HIT_RATE_PERCENT,
        ThreadType::Background => CACHE_BACKGROUND_HIT_RATE_PERCENT,
        ThreadType::MergerTokio => CACHE_BACKGROUND_HIT_RATE_PERCENT,
    }
}

impl Display for CacheAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hit => write!(f, "hits"),
            Self::Miss => write!(f, "misses"),
        }
    }
}

/// Cache counts that can be accessed atomically for multithread updates.
#[derive(Debug, Default)]
pub struct AtomicCacheCounts {
    /// Number of accessed blocks.
    count: AtomicU64,

    /// Total bytes across all the accesses.
    bytes: AtomicU64,

    /// Elapsed time, in nanoseconds.
    elapsed_ns: AtomicU64,
}

impl AtomicCacheCounts {
    pub fn record(&self, duration: Duration, location: BlockLocation) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes
            .fetch_add(location.size as u64, Ordering::Relaxed);
        self.elapsed_ns
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Reads out the counts for processing.
    fn read(&self) -> CacheCounts {
        CacheCounts {
            count: self.count.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            elapsed: Duration::from_nanos(self.elapsed_ns.load(Ordering::Relaxed)),
        }
    }
}

/// Cache access statistics.
#[derive(Copy, Clone, Debug, Default)]
pub struct CacheStats(pub EnumMap<ThreadType, EnumMap<CacheAccess, CacheCounts>>);

impl CacheStats {
    /// Adds metadata for these cache statistics to `meta`, if they are nonzero.
    pub fn metadata(&self, meta: &mut OperatorMeta) {
        for (thread_type, accesses) in &self.0 {
            if !accesses.values().all(CacheCounts::is_empty) {
                meta.extend(accesses.iter().map(|(access, counts)| {
                    MetricReading::new(
                        cache_metric(thread_type, access),
                        Vec::new(),
                        counts.meta_item(),
                    )
                }));

                let hits = accesses[CacheAccess::Hit].count;
                let misses = accesses[CacheAccess::Miss].count;
                meta.extend([MetricReading::new(
                    cache_hit_rate_metric(thread_type),
                    Vec::new(),
                    MetaItem::Percent {
                        numerator: hits,
                        denominator: hits + misses,
                    },
                )]);
            }
        }
    }
}

impl Add for CacheStats {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.add_assign(rhs);
        self
    }
}

impl AddAssign for CacheStats {
    fn add_assign(&mut self, rhs: Self) {
        for (thread_type, accesses) in &mut self.0 {
            for (access, counts) in accesses {
                *counts += rhs.0[thread_type][access];
            }
        }
    }
}

/// Cache counts.
#[derive(Copy, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CacheCounts {
    /// Number of accessed blocks.
    pub count: u64,

    /// Total bytes across all the accesses.
    pub bytes: u64,

    /// Elapsed time.
    pub elapsed: Duration,
}

impl CacheCounts {
    /// Are the counts all zero?
    pub fn is_empty(&self) -> bool {
        self.count == 0 && self.bytes == 0
    }

    /// Returns a [MetaItem] for these cache counts.
    pub fn meta_item(&self) -> MetaItem {
        MetaItem::CacheCounts(*self)
    }
}

impl Add for CacheCounts {
    type Output = Self;

    fn add(mut self, rhs: Self) -> Self::Output {
        self.add_assign(rhs);
        self
    }
}

impl AddAssign for CacheCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.count += rhs.count;
        self.bytes += rhs.bytes;
        self.elapsed += rhs.elapsed;
    }
}
