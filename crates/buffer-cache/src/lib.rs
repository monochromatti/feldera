//! Sharded, weighted in-memory cache with a SIEVE-inspired eviction scan.
//!
//! This crate adapts the NSDI'24 SIEVE design to Feldera's storage cache needs:
//! weighted capacity accounting, per-shard locking, and `Arc`-shared values.
//! The core idea from the paper is preserved: read path only needs to set one
//! visited bit, and a moving hand evicts the first entry that remains unvisited
//! after a scan. This makes things scale nicely because the bit is per-entry
//! and reads have little contention on cache-lines as we don't manage the queues
//! on the read path.
//!
//! Paper: <https://junchengyang.com/publication/nsdi24-SIEVE.pdf>
//!
//! ## What this implementation actually does
//!
//! - Keys are assigned to shards by `hash(key) & (num_shards - 1)`.
//! - Each shard stores:
//!   - `HashMap<K, usize>` mapping keys to dense indices in `nodes`.
//!   - `Vec<Node<K, V>>` holding keys, `Arc<V>` values, charges, and visited bits.
//!   - A `hand` index for eviction scans over the dense vector.
//!   - Weighted occupancy (`used_charge`) and a per-shard capacity budget.
//! - `get()` still takes a shard read lock, then sets the visited bit. This is
//!   cheaper than LRU-style list mutation, but it is not the paper's no-lock hit path.
//! - Removals use `swap_remove`, so the internal scan order is compact and
//!   index-based instead of the paper's stable linked FIFO queue.
//!
//! ## Attribution
//!
//! Some inspiration and tests were adapted from
//! <https://docs.rs/crate/sieve-cache/latest>, which is MIT licensed.

use crossbeam_utils::CachePadded;
use crossbeam_utils::sync::{ShardedLock, ShardedLockReadGuard, ShardedLockWriteGuard};
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, RandomState};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError};

/// A sharded, weighted, thread-safe SIEVE cache.
pub struct SieveCache<K, V, S = RandomState> {
    shards: Vec<ShardedLock<Shard<K, V, S>>>,
    hash_builder: S,
    total_capacity: usize,
}

impl<K, V, S> SieveCache<K, V, S> {
    /// Default power-of-two shard count used by [`SieveCache::new`].
    pub const DEFAULT_SHARDS: usize = 256;
}

struct Node<K, V> {
    key: K,
    value: Arc<V>,
    visited: CachePadded<AtomicBool>,
    charge: usize,
}

struct Shard<K, V, S> {
    /// Key lookup table. Values point to indices in `nodes`.
    map: HashMap<K, usize, S>,
    /// Dense node storage scanned by the eviction hand.
    ///
    /// Unlike the paper's linked FIFO queue, removals here use `swap_remove`
    /// to keep deletion O(1), so indices are compact but not stable.
    nodes: Vec<Node<K, V>>,
    /// Next scan position in `nodes`.
    ///
    /// This is the vector-backed analogue of the paper's moving hand.
    hand: Option<usize>,
    /// Weighted occupancy in bytes for this shard.
    used_charge: usize,
    /// Total shard capacity.
    capacity: usize,
}

impl<K, V, S> Shard<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    fn new(hash_builder: S, capacity: usize) -> Self {
        Self {
            map: HashMap::with_hasher(hash_builder),
            nodes: Vec::new(),
            hand: None,
            used_charge: 0,
            capacity,
        }
    }

    fn insert(&mut self, key: K, value: V, charge: usize) -> Option<Arc<V>> {
        if let Some(&idx) = self.map.get(&key) {
            // Replacement keeps the slot and marks it visited, which mirrors a
            // hit plus overwrite in this implementation.
            let (prev, old_charge) = {
                let node = &mut self.nodes[idx];
                let prev = node.value.clone();
                let old_charge = node.charge;
                node.value = Arc::new(value);
                node.charge = charge;
                node.visited.store(true, Ordering::Relaxed);
                (prev, old_charge)
            };
            self.used_charge = self.used_charge.saturating_sub(old_charge);
            self.used_charge = self.used_charge.saturating_add(charge);
            self.evict_until_within_capacity();
            return Some(prev);
        }

        let idx = self.nodes.len();
        self.nodes.push(Node {
            key: key.clone(),
            value: Arc::new(value),
            // Fresh entries start unvisited, matching Algorithm 1 in the paper.
            visited: CachePadded::new(AtomicBool::new(false)),
            charge,
        });
        self.map.insert(key, idx);
        self.used_charge = self.used_charge.saturating_add(charge);
        self.evict_until_within_capacity();
        None
    }

    fn get(&self, key: &K) -> Option<Arc<V>> {
        let idx = self.map.get(key).copied()?;
        let node = &self.nodes[idx];
        // This is the lazy-promotion part of SIEVE: a hit flips one bit
        // instead of mutating an LRU list.
        node.visited.store(true, Ordering::Relaxed);
        Some(node.value.clone())
    }

    fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        let idx = self.map.get(key).copied()?;
        let removed = self.remove_at_index(idx);
        Some(removed.value)
    }

    fn remove_if<F>(&mut self, predicate: F) -> usize
    where
        F: Fn(&K) -> bool,
    {
        let keys: Vec<K> = self
            .map
            .keys()
            .filter(|key| predicate(key))
            .cloned()
            .collect();
        let mut removed = 0;
        for key in keys {
            if let Some(idx) = self.map.get(&key).copied() {
                let _ = self.remove_at_index(idx);
                removed += 1;
            }
        }
        removed
    }

    fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn usage(&self) -> (usize, usize) {
        (self.used_charge, self.capacity)
    }

    fn evict_until_within_capacity(&mut self) {
        while self.used_charge > self.capacity {
            // The paper's hand acts as a sieve: one sweep can clear survivor
            // bits, and the next sweep can then find an unvisited victim.
            if !self.evict_one() && !self.evict_one() {
                break;
            }
        }
    }

    fn evict_one(&mut self) -> bool {
        if self.nodes.is_empty() {
            return false;
        }

        // We store nodes densely in a vector and scan it backwards from the
        // last remembered hand position. Since new inserts append at the tail,
        // a fresh unvisited entry can be selected before older entries.
        let mut current_idx = self.hand.unwrap_or(self.nodes.len() - 1);
        let start_idx = current_idx;
        let mut wrapped = false;
        let mut found_idx = None;

        loop {
            if !self.nodes[current_idx]
                .visited
                .swap(false, Ordering::Relaxed)
            {
                // First unvisited node is evicted.
                found_idx = Some(current_idx);
                break;
            }

            // Visited nodes get a second chance: clear the bit and keep scanning.
            current_idx = if current_idx > 0 {
                current_idx - 1
            } else {
                if wrapped {
                    break;
                }
                wrapped = true;
                self.nodes.len() - 1
            };

            if current_idx == start_idx {
                break;
            }
        }

        if let Some(idx) = found_idx {
            let _ = self.remove_at_index(idx);
            true
        } else {
            false
        }
    }

    fn remove_at_index(&mut self, idx: usize) -> Node<K, V> {
        let last_idx = self.nodes.len() - 1;

        // Keep the hand valid as indices shift.
        if let Some(hand_idx) = self.hand {
            if hand_idx == idx {
                self.hand = if idx > 0 {
                    Some(idx - 1)
                } else if self.nodes.len() > 1 {
                    Some(self.nodes.len() - 2)
                } else {
                    None
                };
            } else if hand_idx == last_idx && idx != last_idx {
                self.hand = Some(idx);
            }
        }

        // O(1) removal with swap_remove; then repair moved index in map.
        let removed = if idx == last_idx {
            self.nodes.pop().expect("index must exist")
        } else {
            let moved_key = self.nodes[last_idx].key.clone();
            let removed = self.nodes.swap_remove(idx);
            self.map.insert(moved_key, idx);
            removed
        };

        self.map.remove(&removed.key);
        self.used_charge = self.used_charge.saturating_sub(removed.charge);

        if self.nodes.is_empty() {
            self.hand = None;
        }

        removed
    }
}

impl<K, V> SieveCache<K, V, RandomState>
where
    K: Eq + Hash + Clone,
    V: Send + Sync + 'static,
{
    /// Creates a cache with [`Self::DEFAULT_SHARDS`] shards.
    ///
    /// `total_capacity_bytes` is split across shards, with any remainder assigned
    /// to the lowest-index shards.
    pub fn new(total_capacity_bytes: usize) -> Self {
        Self::with_hasher(
            total_capacity_bytes,
            SieveCache::<K, V>::DEFAULT_SHARDS,
            RandomState::new(),
        )
    }

    /// Creates a cache with an explicit shard count.
    ///
    /// # Panics
    ///
    /// Panics if `num_shards == 0` or if `num_shards` is not a power of two.
    pub fn with_shards(total_capacity_bytes: usize, num_shards: usize) -> Self {
        Self::with_hasher(total_capacity_bytes, num_shards, RandomState::new())
    }
}

impl<K, V, S> SieveCache<K, V, S>
where
    K: Eq + Hash + Clone,
    V: Send + Sync + 'static,
    S: BuildHasher + Clone,
{
    /// Creates a cache with an explicit shard count and hash builder.
    ///
    /// `total_capacity_bytes` is divided across shards as evenly as possible.
    ///
    /// # Panics
    ///
    /// Panics if `num_shards == 0` or if `num_shards` is not a power of two.
    pub fn with_hasher(total_capacity_bytes: usize, num_shards: usize, hash_builder: S) -> Self {
        assert!(num_shards > 0, "num_shards must be > 0");
        assert!(
            num_shards.is_power_of_two(),
            "num_shards must be a power of two"
        );

        let base_capacity = total_capacity_bytes / num_shards;
        let remainder = total_capacity_bytes % num_shards;
        let shards = (0..num_shards)
            .map(|idx| {
                // Deterministically spread remainder across low-index shards.
                let shard_capacity = base_capacity + usize::from(idx < remainder);
                ShardedLock::new(Shard::new(hash_builder.clone(), shard_capacity))
            })
            .collect();

        Self {
            shards,
            hash_builder,
            total_capacity: total_capacity_bytes,
        }
    }

    /// Inserts or replaces `key` with a caller-supplied weighted `charge`.
    ///
    /// Returns the previous value if the key was already present.
    ///
    /// If the insert pushes the shard over capacity, eviction runs before this
    /// call returns. An oversized entry may therefore be admitted and then
    /// immediately evicted.
    pub fn insert(&self, key: K, value: V, charge: usize) -> Option<Arc<V>> {
        let shard_idx = self.shard_index(&key);
        let mut shard = self.lock_shard_write(shard_idx);
        shard.insert(key, value, charge)
    }

    /// Looks up `key`, marks the entry visited, and returns a shared handle.
    pub fn get(&self, key: &K) -> Option<Arc<V>> {
        let shard_idx = self.shard_index(key);
        let shard = self.lock_shard_read(shard_idx);
        shard.get(key)
    }

    /// Removes `key` if present and returns the removed value.
    pub fn remove(&self, key: &K) -> Option<Arc<V>> {
        let shard_idx = self.shard_index(key);
        let mut shard = self.lock_shard_write(shard_idx);
        shard.remove(key)
    }

    /// Removes all entries matching `predicate` and returns the number removed.
    pub fn remove_if<F>(&self, predicate: F) -> usize
    where
        F: Fn(&K) -> bool,
    {
        let mut removed = 0;
        for shard_mutex in &self.shards {
            let mut shard = self.lock_shard_write_ref(shard_mutex);
            removed += shard.remove_if(&predicate);
        }
        removed
    }

    /// Returns `true` if `key` is currently present.
    pub fn contains_key(&self, key: &K) -> bool {
        let shard_idx = self.shard_index(key);
        self.lock_shard_read(shard_idx).contains_key(key)
    }

    /// Returns the current number of live entries across all shards.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| self.lock_shard_read_ref(shard).len())
            .sum()
    }

    /// Returns `true` when the cache has no live entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the current total weighted charge across all shards.
    pub fn total_charge(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| self.lock_shard_read_ref(shard).used_charge)
            .sum()
    }

    /// Returns the configured total weighted capacity.
    pub fn total_capacity(&self) -> usize {
        self.total_capacity
    }

    /// Returns the number of shards in the cache.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Returns `(used_charge, capacity)` for shard `idx`.
    ///
    /// # Panics
    ///
    /// Panics if `idx >= self.shard_count()`.
    pub fn shard_usage(&self, idx: usize) -> (usize, usize) {
        let shard = self.lock_shard_read(idx);
        shard.usage()
    }

    fn shard_index(&self, key: &K) -> usize {
        (self.hash_builder.hash_one(key) as usize) & (self.shards.len() - 1)
    }

    fn lock_shard_write(&self, idx: usize) -> ShardedLockWriteGuard<'_, Shard<K, V, S>> {
        self.lock_shard_write_ref(&self.shards[idx])
    }

    fn lock_shard_read(&self, idx: usize) -> ShardedLockReadGuard<'_, Shard<K, V, S>> {
        self.lock_shard_read_ref(&self.shards[idx])
    }

    fn lock_shard_write_ref<'a>(
        &self,
        shard: &'a ShardedLock<Shard<K, V, S>>,
    ) -> ShardedLockWriteGuard<'a, Shard<K, V, S>> {
        shard.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn lock_shard_read_ref<'a>(
        &self,
        shard: &'a ShardedLock<Shard<K, V, S>>,
    ) -> ShardedLockReadGuard<'a, Shard<K, V, S>> {
        shard.read().unwrap_or_else(PoisonError::into_inner)
    }

    #[cfg(test)]
    fn validate_invariants(&self) {
        for shard_mutex in &self.shards {
            let shard = self.lock_shard_read_ref(shard_mutex);
            let live_sum: usize = shard.nodes.iter().map(|node| node.charge).sum();
            assert_eq!(
                shard.map.len(),
                shard.nodes.len(),
                "map/nodes length mismatch"
            );
            assert_eq!(shard.used_charge, live_sum, "used charge mismatch");
            for (key, idx) in &shard.map {
                assert!(
                    *key == shard.nodes[*idx].key,
                    "index map points to wrong key"
                );
            }
            if !shard.map.is_empty() {
                assert!(
                    shard.used_charge <= shard.capacity,
                    "used {} exceeds capacity {}",
                    shard.used_charge,
                    shard.capacity
                );
            }
        }
    }

    #[cfg(test)]
    fn entry_visited(&self, key: &K) -> Option<bool> {
        let shard = self.lock_shard_read(self.shard_index(key));
        let idx = shard.map.get(key).copied()?;
        Some(shard.nodes[idx].visited.load(Ordering::Relaxed))
    }

    #[cfg(test)]
    fn set_entry_visited(&self, key: &K, visited: bool) {
        let shard = self.lock_shard_read(self.shard_index(key));
        if let Some(idx) = shard.map.get(key).copied() {
            shard.nodes[idx].visited.store(visited, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SieveCache;
    use rand::{Rng, SeedableRng, rngs::StdRng};
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic_insert_get_remove() {
        let cache = SieveCache::<u64, String>::with_shards(1024, 4);
        assert!(cache.insert(1, "a".to_string(), 8).is_none());
        assert_eq!(&*cache.get(&1).unwrap(), "a");
        assert_eq!(&*cache.remove(&1).unwrap(), "a");
        assert!(cache.get(&1).is_none());
    }

    #[test]
    fn replacement_returns_previous_and_updates_charge() {
        let cache = SieveCache::<u64, String>::with_shards(64, 2);
        assert!(cache.insert(1, "a".to_string(), 10).is_none());
        let prev = cache.insert(1, "b".to_string(), 7).unwrap();
        assert_eq!(&*prev, "a");
        assert_eq!(&*cache.get(&1).unwrap(), "b");
        assert_eq!(cache.total_charge(), 7);
        cache.validate_invariants();
    }

    #[test]
    fn weighted_eviction_happens_by_charge() {
        let cache = SieveCache::<u64, &'static str>::with_shards(10, 1);
        cache.insert(1, "a", 6);
        cache.insert(2, "b", 6);
        assert_eq!(cache.total_charge(), 6);
        assert!(!cache.contains_key(&1) || !cache.contains_key(&2));
        cache.validate_invariants();
    }

    #[test]
    fn fresh_insert_can_be_evicted_before_older_unvisited_entry() {
        let cache = SieveCache::<u64, &'static str>::with_shards(8, 1);
        cache.insert(1, "a", 4);
        cache.insert(2, "b", 4);
        cache.set_entry_visited(&1, true);
        cache.set_entry_visited(&2, false);
        cache.insert(3, "c", 4);
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert!(!cache.contains_key(&3));
        cache.validate_invariants();
    }

    #[test]
    fn replacement_does_not_break_index_map() {
        let cache = SieveCache::<u64, &'static str>::with_shards(8, 1);
        cache.insert(1, "old", 4);
        cache.insert(1, "new", 4);
        cache.insert(2, "x", 4);
        cache.insert(3, "y", 4);
        if let Some(v) = cache.get(&1) {
            assert_eq!(*v, "new");
        }
        cache.validate_invariants();
    }

    #[test]
    fn remove_keeps_structure_consistent() {
        let cache = SieveCache::<u64, &'static str>::with_shards(8, 1);
        cache.insert(1, "a", 4);
        cache.insert(2, "b", 4);
        cache.remove(&1);
        cache.insert(3, "c", 4);
        assert!(cache.contains_key(&2) || cache.contains_key(&3));
        cache.validate_invariants();
    }

    #[test]
    fn shard_selection_stability() {
        let cache = SieveCache::<u64, u64>::with_shards(1024, 8);
        for key in 0..100 {
            cache.insert(key, key, 1);
            assert_eq!(cache.shard_index(&key), cache.shard_index(&key));
        }
        cache.validate_invariants();
    }

    #[test]
    fn visited_bit_set_on_get() {
        let cache = SieveCache::<u64, &'static str>::with_shards(8, 1);
        cache.insert(1, "a", 4);
        cache.set_entry_visited(&1, false);
        assert_eq!(cache.entry_visited(&1), Some(false));
        let _ = cache.get(&1);
        assert_eq!(cache.entry_visited(&1), Some(true));
        cache.validate_invariants();
    }

    #[test]
    fn fresh_insert_starts_unvisited() {
        let cache = SieveCache::<u64, &'static str>::with_shards(8, 1);
        cache.insert(1, "a", 4);
        assert_eq!(cache.entry_visited(&1), Some(false));

        cache.insert(1, "b", 4);
        assert_eq!(cache.entry_visited(&1), Some(true));
        cache.validate_invariants();
    }

    #[test]
    fn oversize_entry_policy_is_insert_then_evict() {
        let cache = SieveCache::<u64, &'static str>::with_shards(8, 1);
        cache.insert(1, "big", 32);
        assert_eq!(cache.total_charge(), 0);
        assert!(!cache.contains_key(&1));
        cache.validate_invariants();
    }

    #[test]
    fn concurrency_smoke_test() {
        let cache = Arc::new(SieveCache::<u64, u64>::with_shards(4096, 16));
        let mut threads = Vec::new();
        for tid in 0..8 {
            let cache = cache.clone();
            threads.push(thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(1234 + tid);
                for _ in 0..10_000 {
                    let key = rng.gen_range(0..256);
                    match rng.gen_range(0..3) {
                        0 => {
                            cache.insert(key, key, rng.gen_range(1..32));
                        }
                        1 => {
                            let _ = cache.get(&key);
                        }
                        _ => {
                            let _ = cache.remove(&key);
                        }
                    }
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        cache.validate_invariants();
    }

    #[test]
    fn charge_accounting_after_replacement_and_remove() {
        let cache = SieveCache::<u64, &'static str>::with_shards(100, 2);
        cache.insert(1, "a", 20);
        cache.insert(1, "b", 30);
        assert_eq!(cache.total_charge(), 30);
        cache.remove(&1);
        assert_eq!(cache.total_charge(), 0);
        cache.validate_invariants();
    }

    #[test]
    fn basic_sequence() {
        let cache = SieveCache::<String, String>::with_shards(3, 1);
        assert!(
            cache
                .insert("foo".to_string(), "foocontent".to_string(), 1)
                .is_none()
        );
        assert!(
            cache
                .insert("bar".to_string(), "barcontent".to_string(), 1)
                .is_none()
        );
        assert_eq!(
            cache
                .remove(&"bar".to_string())
                .as_deref()
                .map(String::as_str),
            Some("barcontent")
        );
        assert!(
            cache
                .insert("bar2".to_string(), "bar2content".to_string(), 1)
                .is_none()
        );
        assert!(
            cache
                .insert("bar3".to_string(), "bar3content".to_string(), 1)
                .is_none()
        );
        assert_eq!(
            cache.get(&"foo".to_string()).as_deref().map(String::as_str),
            Some("foocontent")
        );
        assert_eq!(cache.get(&"bar".to_string()), None);
        assert_eq!(
            cache
                .get(&"bar2".to_string())
                .as_deref()
                .map(String::as_str),
            Some("bar2content")
        );
        assert_eq!(
            cache
                .get(&"bar3".to_string())
                .as_deref()
                .map(String::as_str),
            Some("bar3content")
        );
    }

    #[test]
    fn visited_flag_update() {
        let cache = SieveCache::<String, String>::with_shards(2, 1);
        cache.insert("key1".to_string(), "value1".to_string(), 1);
        cache.insert("key2".to_string(), "value2".to_string(), 1);
        cache.insert("key1".to_string(), "updated".to_string(), 1);
        cache.insert("key3".to_string(), "value3".to_string(), 1);
        assert_eq!(
            cache
                .get(&"key1".to_string())
                .as_deref()
                .map(String::as_str),
            Some("updated")
        );
        cache.validate_invariants();
    }

    #[test]
    fn insert_never_exceeds_capacity_when_all_visited() {
        let cache = SieveCache::<String, u64>::with_shards(2, 1);
        cache.insert("a".to_string(), 1, 1);
        cache.insert("b".to_string(), 2, 1);
        assert!(cache.get(&"a".to_string()).is_some());
        assert!(cache.get(&"b".to_string()).is_some());
        cache.insert("c".to_string(), 3, 1);
        assert!(cache.len() <= 2);
        assert!(cache.total_charge() <= cache.total_capacity());
    }

    #[test]
    fn shard_capacity_remainder_distribution_is_deterministic() {
        let cache = SieveCache::<u64, u64>::with_shards(10, 4);
        let caps: Vec<usize> = (0..4).map(|i| cache.shard_usage(i).1).collect();
        assert_eq!(caps, vec![3, 3, 2, 2]);
    }

    #[test]
    fn zero_charge_entries_do_not_increase_budget_usage() {
        let cache = SieveCache::<u64, u64>::with_shards(1, 1);
        cache.insert(1, 10, 0);
        cache.insert(2, 20, 0);
        cache.insert(3, 30, 0);
        assert_eq!(cache.total_charge(), 0);
        assert_eq!(cache.len(), 3);
        assert_eq!(*cache.get(&1).unwrap(), 10);
    }
}
