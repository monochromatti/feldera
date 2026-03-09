# feldera-buffer-cache

`feldera-buffer-cache` provides `SieveCache`, a sharded, weighted, thread-safe
in-memory buffer cache used by Feldera's storage layer.

The implementation is inspired by the NSDI'24 SIEVE paper:
<https://junchengyang.com/publication/nsdi24-SIEVE.pdf>

## What this crate implements

- Weighted capacity accounting. Every insert supplies a `charge`, and eviction
  is based on the sum of charges instead of entry count.
- Sharded concurrency. `SieveCache::new(total_capacity)` uses
  `SieveCache::DEFAULT_SHARDS`, while `SieveCache::with_shards(...)` lets the
  caller pick an explicit power-of-two shard count.
- Cheap hit handling. A cache hit only flips a visited bit; it does not move the
  entry through a recency list.
- Shared values. Public operations return `Arc<V>` so callers can keep values
  alive after removal or eviction.

## API sketch

```rust
use feldera_buffer_cache::SieveCache;

let cache = SieveCache::<u64, Vec<u8>>::with_shards(64 << 20, 64);
cache.insert(7, vec![0; 4096], 4096);

let page = cache.get(&7).expect("entry should be present");
assert_eq!(page.len(), 4096);
```

## Benchmarks

Comparisons against `dbsp`'s current LRU buffer cache live under `dbsp` bench
targets so they exercise the real in-tree implementation:

```bash
cargo bench -p dbsp --bench buffer_cache_compare -- --capacity-mib 256
```

```bash
cargo bench -p dbsp --bench buffer_cache_throughput -- --threads 1,2,4,8 --max-duration 10
```

The standalone crate currently only ships the library and its tests; the
cross-cache comparison programs now live entirely under `dbsp/benches`.
