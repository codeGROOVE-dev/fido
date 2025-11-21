package bdcache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFilePersist_StoreLoad(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, int](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Override directory to use temp dir
	fp.dir = dir

	ctx := context.Background()

	// Store a value
	if err := fp.Store(ctx, "key1", 42, time.Time{}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Load the value
	val, expiry, found, err := fp.Load(ctx, "key1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Fatal("key1 not found")
	}
	if val != 42 {
		t.Errorf("Load value = %d; want 42", val)
	}
	if !expiry.IsZero() {
		t.Error("expiry should be zero")
	}
}

func TestFilePersist_LoadMissing(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, int](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()
	fp.dir = dir

	ctx := context.Background()

	// Load non-existent key
	_, _, found, err := fp.Load(ctx, "missing")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if found {
		t.Error("missing key should not be found")
	}
}

func TestFilePersist_TTL(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, string](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()
	fp.dir = dir

	ctx := context.Background()

	// Store with past expiry
	past := time.Now().Add(-1 * time.Second)
	if err := fp.Store(ctx, "expired", "value", past); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Should not be loadable
	_, _, found, err := fp.Load(ctx, "expired")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if found {
		t.Error("expired key should not be found")
	}

	// File should be removed (subdirectory may remain, but should be empty or only have empty subdirs)
	filename := filepath.Join(dir, fp.keyToFilename("expired"))
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Error("expired file should be removed")
	}
}

func TestFilePersist_Delete(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, int](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()
	fp.dir = dir

	ctx := context.Background()

	// Store and delete
	if err := fp.Store(ctx, "key1", 42, time.Time{}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := fp.Delete(ctx, "key1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Should not be loadable
	_, _, found, err := fp.Load(ctx, "key1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if found {
		t.Error("deleted key should not be found")
	}

	// Deleting non-existent key should not error
	if err := fp.Delete(ctx, "missing"); err != nil {
		t.Errorf("Delete missing key: %v", err)
	}
}

func TestFilePersist_LoadAll(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, int](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()
	fp.dir = dir

	ctx := context.Background()

	// Store multiple entries
	entries := map[string]int{
		"key1": 1,
		"key2": 2,
		"key3": 3,
	}
	for k, v := range entries {
		if err := fp.Store(ctx, k, v, time.Time{}); err != nil {
			t.Fatalf("Store %s: %v", k, err)
		}
	}

	// Store expired entry
	if err := fp.Store(ctx, "expired", 99, time.Now().Add(-1*time.Second)); err != nil {
		t.Fatalf("Store expired: %v", err)
	}

	// Load all
	entryCh, errCh := fp.LoadAll(ctx)

	loaded := make(map[string]int)
	for entry := range entryCh {
		loaded[entry.Key] = entry.Value
	}

	// Check for errors
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("LoadAll error: %v", err)
		}
	default:
	}

	// Verify all non-expired entries loaded
	if len(loaded) != len(entries) {
		t.Errorf("loaded %d entries; want %d", len(loaded), len(entries))
	}

	for k, v := range entries {
		if loaded[k] != v {
			t.Errorf("loaded[%s] = %d; want %d", k, loaded[k], v)
		}
	}

	// Expired entry should not be loaded
	if _, ok := loaded["expired"]; ok {
		t.Error("expired entry should not be loaded")
	}
}

func TestFilePersist_Update(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, string](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()
	fp.dir = dir

	ctx := context.Background()

	// Store initial value
	if err := fp.Store(ctx, "key", "value1", time.Time{}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	// Update value
	if err := fp.Store(ctx, "key", "value2", time.Time{}); err != nil {
		t.Fatalf("Store update: %v", err)
	}

	// Load and verify updated value
	val, _, found, err := fp.Load(ctx, "key")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Fatal("key not found")
	}
	if val != "value2" {
		t.Errorf("Load value = %s; want value2", val)
	}
}

func TestFilePersist_ContextCancellation(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, int](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()
	fp.dir = dir

	// Store many entries with valid alphanumeric keys
	ctx := context.Background()
	for i := range 100 {
		key := fmt.Sprintf("key%d", i)
		if err := fp.Store(ctx, key, i, time.Time{}); err != nil {
			t.Fatalf("Store: %v", err)
		}
	}

	// Cancel context during LoadAll
	ctx, cancel := context.WithCancel(context.Background())
	entryCh, errCh := fp.LoadAll(ctx)

	// Read a few entries, then cancel
	count := 0
	cancel()

	for range entryCh {
		count++
	}

	// Should get context cancellation error
	err = <-errCh
	if err == nil || !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error; got %v", err)
	}
}

func TestFilePersist_Store_CompleteFlow(t *testing.T) {
	dir := t.TempDir()
	fp, err := newFilePersist[string, string](filepath.Base(dir))
	if err != nil {
		t.Fatalf("newFilePersist: %v", err)
	}
	defer func() {
		if err := fp.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()
	fp.dir = dir

	ctx := context.Background()

	// Test complete store flow with expiry
	expiry := time.Now().Add(1 * time.Hour)
	if err := fp.Store(ctx, "key1", "value1", expiry); err != nil {
		t.Fatalf("Store with expiry: %v", err)
	}

	// Load and verify expiry is set
	val, loadedExpiry, found, err := fp.Load(ctx, "key1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Fatal("key1 not found")
	}
	if val != "value1" {
		t.Errorf("value = %s; want value1", val)
	}

	// Verify expiry was stored correctly (within 1 second)
	if loadedExpiry.Sub(expiry).Abs() > time.Second {
		t.Errorf("expiry = %v; want ~%v", loadedExpiry, expiry)
	}
}

func TestCache_Set_StressTest_10000Items(t *testing.T) {
	ctx := context.Background()
	cacheID := fmt.Sprintf("test-set-stress-%d", time.Now().Unix())

	cache, err := New[int, string](ctx, WithLocalStore(cacheID))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
		if baseDir, err := os.UserCacheDir(); err == nil {
			if err := os.RemoveAll(baseDir + "/" + cacheID); err != nil {
				t.Logf("Failed to clean up test dir: %v", err)
			}
		}
	}()

	// Write 10,000 items synchronously
	const count = 10000
	startWrite := time.Now()
	for i := range count {
		if err := cache.Set(ctx, i, fmt.Sprintf("value-%d", i), 0); err != nil {
			t.Fatalf("Set[%d]: %v", i, err)
		}
	}
	writeTime := time.Since(startWrite)
	t.Logf("Wrote %d items synchronously in %v (%.2f items/sec)", count, writeTime, float64(count)/writeTime.Seconds())

	// All should be immediately available in memory
	for i := range count {
		val, found, err := cache.Get(ctx, i)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		if !found {
			t.Errorf("expected key %d to be in memory immediately", i)
		}
		expected := fmt.Sprintf("value-%d", i)
		if val != expected {
			t.Errorf("key %d: expected %s, got %s", i, expected, val)
		}
	}
	t.Logf("All %d items verified in memory", count)

	// Create new cache instance to verify persistence (should be immediate with Set)
	cache2, err := New[int, string](ctx, WithLocalStore(cacheID))
	if err != nil {
		t.Fatalf("New second cache: %v", err)
	}
	defer func() {
		if err := cache2.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Check a statistically significant sample (100 random items)
	persistedCount := 0
	sampleSize := 100
	for i := range sampleSize {
		key := i * (count / sampleSize) // Evenly distributed sample
		val, found, err := cache2.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get[%d] from second cache: %v", key, err)
		}
		if found {
			persistedCount++
			expected := fmt.Sprintf("value-%d", key)
			if val != expected {
				t.Errorf("key %d: expected %s, got %s", key, expected, val)
			}
		}
	}

	persistRatio := float64(persistedCount) / float64(sampleSize)
	t.Logf("Persistence success rate: %d/%d (%.1f%%)", persistedCount, sampleSize, persistRatio*100)

	// We expect 100% of items to be persisted with synchronous Set
	if persistRatio < 1.0 {
		t.Errorf("expected 100%% persistence success with synchronous Set, got %.1f%%", persistRatio*100)
	}
}

func TestCache_SetAsync_StressTest_10000Items(t *testing.T) {
	ctx := context.Background()
	cacheID := fmt.Sprintf("test-setasync-stress-%d", time.Now().Unix())

	cache, err := New[int, string](ctx, WithLocalStore(cacheID))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cacheClosed := false
	defer func() {
		if !cacheClosed {
			if err := cache.Close(); err != nil {
				t.Logf("Close error: %v", err)
			}
		}
		if baseDir, err := os.UserCacheDir(); err == nil {
			if err := os.RemoveAll(baseDir + "/" + cacheID); err != nil {
				t.Logf("Failed to clean up test dir: %v", err)
			}
		}
	}()

	// Write 10,000 items asynchronously in rapid succession
	const count = 10000
	startWrite := time.Now()
	for i := range count {
		if err := cache.SetAsync(ctx, i, fmt.Sprintf("value-%d", i), 0); err != nil {
			t.Fatalf("SetAsync[%d]: %v", i, err)
		}
	}
	writeTime := time.Since(startWrite)
	t.Logf("Wrote %d items in %v (%.2f items/sec)", count, writeTime, float64(count)/writeTime.Seconds())

	// All should be immediately available in memory
	for i := range count {
		val, found, err := cache.Get(ctx, i)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		if !found {
			t.Errorf("expected key %d to be in memory immediately", i)
		}
		expected := fmt.Sprintf("value-%d", i)
		if val != expected {
			t.Errorf("key %d: expected %s, got %s", i, expected, val)
		}
	}
	t.Logf("All %d items verified in memory", count)

	// Close the first cache to ensure all async operations complete
	if err := cache.Close(); err != nil {
		t.Fatalf("Close first cache: %v", err)
	}
	cacheClosed = true

	// Wait for async goroutines to complete
	// Based on sync Set performance (~1.15s for 10k items), wait 3x that time for safety
	// This accounts for system contention when running full test suite
	t.Log("Waiting for async goroutines to complete...")
	time.Sleep(9 * time.Second)

	// Create new cache instance to verify persistence
	cache2, err := New[int, string](ctx, WithLocalStore(cacheID))
	if err != nil {
		t.Fatalf("New second cache: %v", err)
	}
	defer func() {
		if err := cache2.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Check a statistically significant sample (100 random items)
	persistedCount := 0
	sampleSize := 100
	for i := range sampleSize {
		key := i * (count / sampleSize) // Evenly distributed sample
		val, found, err := cache2.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get[%d] from second cache: %v", key, err)
		}
		if found {
			persistedCount++
			expected := fmt.Sprintf("value-%d", key)
			if val != expected {
				t.Errorf("key %d: expected %s, got %s", key, expected, val)
			}
		}
	}

	persistRatio := float64(persistedCount) / float64(sampleSize)
	t.Logf("Persistence success rate: %d/%d (%.1f%%)", persistedCount, sampleSize, persistRatio*100)

	// We expect at least 95% of items to be persisted
	if persistRatio < 0.95 {
		t.Errorf("expected at least 95%% persistence success, got %.1f%%", persistRatio*100)
	}
}
