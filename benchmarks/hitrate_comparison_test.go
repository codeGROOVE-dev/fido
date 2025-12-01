package benchmarks

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/codeGROOVE-dev/bdcache"

	lru "github.com/hashicorp/golang-lru/v2"
)

// This file contains hit rate comparison benchmarks designed to expose
// the differences between S3-FIFO and LRU eviction algorithms.

const cacheSize = 10000

// zipf generates a Zipf-distributed random number in [0, n)
// Lower values are more likely (simulates hot items being accessed more)
func zipf(rng *rand.Rand, n int, s float64) int {
	// Zipf distribution: P(k) ∝ 1/k^s
	u := rng.Float64()
	return int(math.Floor(float64(n) * math.Pow(u, 1.0/(1.0-s))))
}

// Workload 1: One-hit wonders mixed with hot items (Zipf distribution)
// S3-FIFO should win because one-hit wonders stay in Small queue and don't evict hot items.
// LRU treats one-hit wonders the same as hot items, causing unnecessary evictions.
func generateOneHitWonderWorkload(n int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	keys := make([]int, n)
	hotSetSize := 12000 // Slightly larger than cache to force evictions
	oneHitWonderID := 100000

	for i := range n {
		if rng.Float64() < 0.3 {
			// 30% one-hit wonders - each unique, accessed once
			keys[i] = oneHitWonderID
			oneHitWonderID++
		} else {
			// 70% hot set with Zipf distribution (some items much hotter)
			keys[i] = zipf(rng, hotSetSize, 0.7)
		}
	}
	return keys
}

// Workload 2: Burst scan pattern
// Periodic burst scans through cold data that would flush LRU but not S3-FIFO.
func generateScanWorkload(n int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	keys := make([]int, n)
	workingSet := 8000
	scanSize := 15000    // Large scan
	scanBurstSize := 500 // Items per burst

	i := 0
	scanOffset := 100000
	for i < n {
		// Working set phase: access with Zipf distribution
		burstLen := 1000 + rng.Intn(500)
		for j := 0; j < burstLen && i < n; j++ {
			keys[i] = zipf(rng, workingSet, 0.8)
			i++
		}

		// Scan burst: sequential access through cold data
		if rng.Float64() < 0.2 && i < n { // 20% chance of scan burst
			for j := 0; j < scanBurstSize && i < n; j++ {
				keys[i] = scanOffset + (rng.Intn(scanSize))
				i++
			}
		}
	}
	return keys
}

// Workload 3: Loop pattern with pollution bursts
// Access pattern: repeatedly loop through working set, with occasional pollution bursts
// S3-FIFO should keep the loop items even when polluted.
func generateLoopWorkload(n int, seed int64) []int {
	rng := rand.New(rand.NewSource(seed))
	keys := make([]int, n)
	loopSize := 9000 // Close to cache size

	i := 0
	pollutionID := 200000
	loopPos := 0
	for i < n {
		// Loop phase: sequential access through working set
		loopLen := 500 + rng.Intn(500)
		for j := 0; j < loopLen && i < n; j++ {
			keys[i] = loopPos % loopSize
			loopPos++
			i++
		}

		// Pollution burst: unique items accessed once
		if rng.Float64() < 0.15 && i < n { // 15% chance of pollution burst
			burstSize := 200 + rng.Intn(300)
			for j := 0; j < burstSize && i < n; j++ {
				keys[i] = pollutionID
				pollutionID++
				i++
			}
		}
	}
	return keys
}

// runCacheWorkload executes a workload and returns hit rate
func runCacheWorkload(workload []int, cacheName string) float64 {
	ctx := context.Background()
	var hits, misses int

	switch cacheName {
	case "bdcache":
		cache, err := bdcache.New[int, int](ctx, bdcache.WithMemorySize(cacheSize))
		if err != nil {
			return 0
		}

		for _, key := range workload {
			if _, found, err := cache.Get(ctx, key); err == nil && found {
				hits++
			} else {
				misses++
				_ = cache.Set(ctx, key, key, 0) //nolint:errcheck // benchmark code
			}
		}

	case "golang-lru":
		cache, err := lru.New[int, int](cacheSize)
		if err != nil {
			return 0
		}

		for _, key := range workload {
			if _, found := cache.Get(key); found {
				hits++
			} else {
				misses++
				cache.Add(key, key)
			}
		}
	}

	return float64(hits) / float64(hits+misses) * 100
}

// Benchmark: One-hit wonders
func BenchmarkHitRate_OneHitWonders_bdcache(b *testing.B) {
	workload := generateOneHitWonderWorkload(100000, 42)
	b.ResetTimer()
	for range b.N {
		hitRate := runCacheWorkload(workload, "bdcache")
		b.ReportMetric(hitRate, "hit%")
	}
}

func BenchmarkHitRate_OneHitWonders_LRU(b *testing.B) {
	workload := generateOneHitWonderWorkload(100000, 42)
	b.ResetTimer()
	for range b.N {
		hitRate := runCacheWorkload(workload, "golang-lru")
		b.ReportMetric(hitRate, "hit%")
	}
}

// Benchmark: Scan resistance
func BenchmarkHitRate_Scan_bdcache(b *testing.B) {
	workload := generateScanWorkload(100000, 42)
	b.ResetTimer()
	for range b.N {
		hitRate := runCacheWorkload(workload, "bdcache")
		b.ReportMetric(hitRate, "hit%")
	}
}

func BenchmarkHitRate_Scan_LRU(b *testing.B) {
	workload := generateScanWorkload(100000, 42)
	b.ResetTimer()
	for range b.N {
		hitRate := runCacheWorkload(workload, "golang-lru")
		b.ReportMetric(hitRate, "hit%")
	}
}

// Benchmark: Loop with pollution
func BenchmarkHitRate_Loop_bdcache(b *testing.B) {
	workload := generateLoopWorkload(100000, 42)
	b.ResetTimer()
	for range b.N {
		hitRate := runCacheWorkload(workload, "bdcache")
		b.ReportMetric(hitRate, "hit%")
	}
}

func BenchmarkHitRate_Loop_LRU(b *testing.B) {
	workload := generateLoopWorkload(100000, 42)
	b.ResetTimer()
	for range b.N {
		hitRate := runCacheWorkload(workload, "golang-lru")
		b.ReportMetric(hitRate, "hit%")
	}
}

// Comparison test that runs all workloads and prints results
func TestHitRateComparison(t *testing.T) {
	seed := int64(42)
	workloads := []struct {
		name     string
		workload []int
	}{
		{"One-hit wonders (Zipf + 30% unique)", generateOneHitWonderWorkload(100000, seed)},
		{"Scan resistance (burst scans)", generateScanWorkload(100000, seed)},
		{"Loop pollution (sequential + bursts)", generateLoopWorkload(100000, seed)},
	}

	fmt.Println("\nHit Rate Comparison: bdcache (S3-FIFO) vs golang-lru (LRU)")
	fmt.Println("Cache size: 10,000 items | Workload size: 100,000 operations")
	fmt.Println("================================================================================")

	for _, w := range workloads {
		bdcacheRate := runCacheWorkload(w.workload, "bdcache")
		lruRate := runCacheWorkload(w.workload, "golang-lru")
		diff := bdcacheRate - lruRate

		fmt.Printf("\n%s:\n", w.name)
		fmt.Printf("  bdcache (S3-FIFO): %.2f%%\n", bdcacheRate)
		fmt.Printf("  golang-lru (LRU):  %.2f%%\n", lruRate)
		switch {
		case diff > 0.5:
			fmt.Printf("  ✅ bdcache wins by %.2f percentage points\n", diff)
		case diff < -0.5:
			fmt.Printf("  ❌ LRU wins by %.2f percentage points\n", -diff)
		default:
			fmt.Printf("  🤝 Tie (within 0.5%%)\n")
		}
	}
	fmt.Println()
}
