# multicache

A sharded in-memory cache for Go with optional persistence.

Implements S3-FIFO from ["FIFO queues are all you need for cache eviction"](https://dl.acm.org/doi/10.1145/3600006.3613147) (SOSP'23). S3-FIFO matches or exceeds LRU hit rates with simpler operations and better concurrency.

## Install

```
go get github.com/codeGROOVE-dev/multicache
```

## Use

```go
cache := multicache.New[string, int](multicache.Size(10000))
cache.Set("answer", 42)
val, ok := cache.Get("answer")
```

With persistence:

```go
store, _ := localfs.New[string, User]("myapp", "")
cache, _ := multicache.NewTiered(store)

cache.Set(ctx, "user:123", user)           // sync write
cache.SetAsync(ctx, "user:456", user)      // async write
```

GetSet deduplicates concurrent loads:

```go
user, err := cache.GetSet("user:123", func() (User, error) {
    return db.LoadUser("123")
})
```

## Options

```go
multicache.Size(n)           // max entries (default 16384)
multicache.TTL(time.Hour)    // default expiration
```

## Persistence

Memory cache backed by durable storage. Reads check memory first; writes go to both.

| Backend | Import |
|---------|--------|
| Local filesystem | `pkg/store/localfs` |
| Valkey/Redis | `pkg/store/valkey` |
| Google Cloud Datastore | `pkg/store/datastore` |
| Auto-detect (Cloud Run) | `pkg/store/cloudrun` |

All backends support S2 or Zstd compression via `pkg/store/compress`.

## Performance

[gocachemark](https://github.com/tstromberg/gocachemark) compares cache libraries across hit rate, latency, throughput, and memory. Overall scores (Dec 2025):

```
#1  multicache   160 points (13 gold, 3 silver, 1 bronze)
#2  otter         80 points
#3  clock         70 points
```

Where multicache wins:

- **Hit rate**: Wins 6 of 9 production traces. +4.9% on Meta, +4.1% on Tencent Photo, +1.6% on Wikipedia
- **Throughput**: 12x faster Set, 1.6x faster Get vs otter at 32 threads
- **Latency**: 9-11ns Get, zero allocations

Where others win:

- **Memory**: freelru and otter use less memory per entry
- **Some traces**: CLOCK/LRU marginally better on purely temporal workloads (IBM Docker, Thesios)

Run `make bench` or see gocachemark for full results.

## Algorithm

S3-FIFO uses three queues: small (new entries), main (promoted entries), and ghost (recently evicted keys). New items enter small; items accessed twice move to main. The ghost queue tracks evicted keys in a bloom filter to fast-track their return.

This implementation adds:

- **Dynamic sharding** - scales to 16×GOMAXPROCS shards; at 32 threads: 21x Get throughput, 6x Set throughput vs single shard
- **Tuned small queue** - 24.7% vs paper's 10%, chosen via sweep in 0.1% increments to maximize wins across 9 production traces
- **Full ghost frequency restoration** - returning keys restore 100% of their previous access count; +0.37% zipf, +0.05% meta, +0.04% tencentPhoto, +0.03% wikipedia
- **Extended frequency cap** - max freq=7 vs paper's 3; +0.9% meta, +0.8% zipf
- **Hot item demotion** - items that were once hot (freq≥4) get demoted to small queue instead of evicted; +0.24% zipf
- **Death row buffer** - 8-entry buffer per shard holds recently evicted items for instant resurrection; +0.04% meta/tencentPhoto, +0.03% wikipedia, +8% set throughput
- **Ghost frequency ring buffer** - fixed-size 256-entry ring replaces map allocations; -5.1% string latency, -44.5% memory
- **Cached entry hash** - hash computed once at set, reused on eviction; eliminates re-hashing overhead

Memory overhead is ~66 bytes/item (vs 38 for CLOCK, 119 for map-based ghost tracking)

Details: [s3fifo.com](https://s3fifo.com/)

## License

Apache 2.0
