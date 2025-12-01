# bdcache - Big Dumb Cache

<img src="media/logo-small.png" alt="bdcache logo" width="256">

[![Go Reference](https://pkg.go.dev/badge/github.com/codeGROOVE-dev/bdcache.svg)](https://pkg.go.dev/github.com/codeGROOVE-dev/bdcache)
[![Go Report Card](https://goreportcard.com/badge/github.com/codeGROOVE-dev/bdcache)](https://goreportcard.com/report/github.com/codeGROOVE-dev/bdcache)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

<br clear="right">

Fast, persistent Go cache with S3-FIFO eviction - better hit rates than LRU, survives restarts with pluggable persistence backends, zero allocations.

## Install

```bash
go get github.com/codeGROOVE-dev/bdcache
```

## Use

```go
import (
    "github.com/codeGROOVE-dev/bdcache"
    "github.com/codeGROOVE-dev/bdcache/persist/localfs"
)

// Memory only
cache, _ := bdcache.New[string, int](ctx)
cache.Set(ctx, "answer", 42, 0)           // Synchronous: returns after persistence completes
cache.SetAsync(ctx, "answer", 42, 0)      // Async: returns immediately, persists in background
val, found, _ := cache.Get(ctx, "answer")

// With local file persistence
p, _ := localfs.New[string, User]("myapp", "")
cache, _ := bdcache.New[string, User](ctx,
    bdcache.WithPersistence(p))

// With Valkey/Redis persistence
p, _ := valkey.New[string, User](ctx, "myapp", "localhost:6379")
cache, _ := bdcache.New[string, User](ctx,
    bdcache.WithPersistence(p))

// Cloud Run auto-detection (datastore in Cloud Run, localfs elsewhere)
p, _ := cloudrun.New[string, User](ctx, "myapp")
cache, _ := bdcache.New[string, User](ctx,
    bdcache.WithPersistence(p))
```

## Features

- **S3-FIFO eviction** - Better than LRU ([learn more](https://s3fifo.com/))
- **Type safe** - Go generics
- **Pluggable persistence** - Bring your own database or use built-in backends:
  - [`persist/localfs`](persist/localfs) - Local files (gob encoding, zero dependencies)
  - [`persist/datastore`](persist/datastore) - Google Cloud Datastore
  - [`persist/valkey`](persist/valkey) - Valkey/Redis
  - [`persist/cloudrun`](persist/cloudrun) - Auto-detect Cloud Run
- **Graceful degradation** - Cache works even if persistence fails
- **Per-item TTL** - Optional expiration

## Performance

For performance, bdcache biases toward:

* the highest hit-rate in real-world workloads
* the lowest CPU overhead for reads (high ns/op)

### CPU Overhead

Benchmarks on MacBook Pro M4 Max comparing memory-only operations:

  | Operation | bdcache     | LRU         | ristretto   | otter       |
  |-----------|-------------|-------------|-------------|-------------|
  | Get       | 8.63 ns/op  | 14.03 ns/op | 30.24 ns/op | 15.25 ns/op |
  | Set       | 14.03 ns/op | 13.94 ns/op | 96.24 ns/op | 141.3 ns/op |

bdcache is faster than anyone else for Get operations, while still faster than most implementations for Set. 

See [benchmarks/](benchmarks/) for detailed methodology and running instructions.

## Hit Rates

For hit rates, bdcache is competitive with otter & tinylfu, often nudging out both depending on the benchmark scenario. Here's an independent benchmark using [scalalang2/go-cache-benchmark](https://github.com/scalalang2/go-cache-benchmark):

```
itemSize=500000, workloads=7500000, cacheSize=1.00%, zipf's alpha=0.99, concurrency=16

       CACHE      | HITRATE |   QPS    |  HITS   | MISSES
------------------+---------+----------+---------+----------
  bdcache         | 64.45%  |  5572065 | 4833482 | 2666518
  tinylfu         | 63.94%  |  2357008 | 4795685 | 2704315
  s3-fifo         | 63.57%  |  2899111 | 4767672 | 2732328
  sieve           | 63.40%  |  2697842 | 4754699 | 2745301
  slru            | 62.88%  |  2655807 | 4715817 | 2784183
  s4lru           | 62.67%  |  2877974 | 4700060 | 2799940
  two-queue       | 61.99%  |  2362205 | 4649519 | 2850481
  otter           | 61.86%  |  9457755 | 4639781 | 2860219
  clock           | 56.11%  |  2956248 | 4208167 | 3291833
  freelru-sharded | 55.45%  | 21067416 | 4159005 | 3340995
  freelru-synced  | 55.38%  |  4244482 | 4153156 | 3346844
  lru-groupcache  | 55.37%  |  2463863 | 4153022 | 3346978
  lru-hashicorp   | 55.36%  |  2776749 | 4152099 | 3347901
  ```
  
The QPS in this benchmark represents mixed Get/Set - otter in particular shines at concurrency

## License

Apache 2.0
