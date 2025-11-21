package bdcache

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestCache_SetAsync_MemoryOnly(t *testing.T) {
	ctx := context.Background()
	cache, err := New[string, int](ctx)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Test SetAsync - should work immediately in memory
	if err := cache.SetAsync(ctx, "key1", 42, 0); err != nil {
		t.Fatalf("SetAsync: %v", err)
	}

	// Should be immediately available in memory
	val, found, err := cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Fatal("expected key1 to be found immediately after SetAsync")
	}
	if val != 42 {
		t.Errorf("expected value 42, got %d", val)
	}
}

func TestCache_SetAsync_WithTTL(t *testing.T) {
	ctx := context.Background()
	cache, err := New[string, int](ctx)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Set with 100ms TTL
	if err := cache.SetAsync(ctx, "key1", 42, 100*time.Millisecond); err != nil {
		t.Fatalf("SetAsync: %v", err)
	}

	// Should be available immediately
	val, found, err := cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || val != 42 {
		t.Fatal("expected key1 to be found with value 42")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should be expired
	_, found, err = cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get after expiry: %v", err)
	}
	if found {
		t.Error("expected key1 to be expired")
	}
}

func TestCache_SetAsync_WithPersistence(t *testing.T) {
	ctx := context.Background()
	cacheID := fmt.Sprintf("test-setasync-%d", time.Now().Unix())

	// Create cache with file persistence
	cache, err := New[string, string](ctx, WithLocalStore(cacheID))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Clean up
	defer func() {
		if err := cache.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
		// Clean up test directory
		if baseDir, err := os.UserCacheDir(); err == nil {
			if err := os.RemoveAll(baseDir + "/" + cacheID); err != nil {
				t.Logf("Failed to clean up test dir: %v", err)
			}
		}
	}()

	// SetAsync should validate and cache in memory synchronously
	if err := cache.SetAsync(ctx, "key1", "value1", 0); err != nil {
		t.Fatalf("SetAsync: %v", err)
	}

	// Should be immediately available in memory
	val, found, err := cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || val != "value1" {
		t.Fatal("expected key1 to be found immediately in memory")
	}

	// Wait for async persistence to complete
	time.Sleep(100 * time.Millisecond)

	// Create new cache instance to verify persistence
	cache2, err := New[string, string](ctx, WithLocalStore(cacheID))
	if err != nil {
		t.Fatalf("New second cache: %v", err)
	}
	defer func() {
		if err := cache2.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Should load from disk
	val2, found2, err := cache2.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get from second cache: %v", err)
	}
	if !found2 {
		t.Fatal("expected key1 to be persisted to disk")
	}
	if val2 != "value1" {
		t.Errorf("expected value1, got %s", val2)
	}
}

func TestCache_SetAsync_InvalidKey(t *testing.T) {
	ctx := context.Background()
	cacheID := fmt.Sprintf("test-setasync-invalid-%d", time.Now().Unix())

	cache, err := New[string, string](ctx, WithLocalStore(cacheID))
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

	// Should return validation error for invalid key
	err = cache.SetAsync(ctx, "../../../etc/passwd", "malicious", 0)
	if err == nil {
		t.Fatal("expected error for invalid key with path traversal")
	}

	// Should not be in cache
	_, found, err := cache.Get(ctx, "../../../etc/passwd")
	if err != nil {
		t.Logf("Get error (expected): %v", err)
	}
	if found {
		t.Error("invalid key should not be cached")
	}
}

func TestCache_SetAsync_HighVolume(t *testing.T) {
	ctx := context.Background()
	cacheID := fmt.Sprintf("test-setasync-volume-%d", time.Now().Unix())

	cache, err := New[int, int](ctx, WithLocalStore(cacheID))
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

	// Write 1000 items asynchronously
	count := 1000
	for i := range count {
		if err := cache.SetAsync(ctx, i, i*2, 0); err != nil {
			t.Fatalf("SetAsync[%d]: %v", i, err)
		}
	}

	// All should be immediately available in memory
	for i := range count {
		val, found, err := cache.Get(ctx, i)
		if err != nil {
			t.Fatalf("Get[%d]: %v", i, err)
		}
		if !found {
			t.Errorf("expected key %d to be in memory immediately", i)
		}
		if val != i*2 {
			t.Errorf("key %d: expected %d, got %d", i, i*2, val)
		}
	}

	// Wait for async persistence to complete
	time.Sleep(500 * time.Millisecond)

	// Verify a sample is persisted
	cache2, err := New[int, int](ctx, WithLocalStore(cacheID))
	if err != nil {
		t.Fatalf("New second cache: %v", err)
	}
	defer func() {
		if err := cache2.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Check a few random items
	for _, i := range []int{0, 100, 500, 999} {
		val, found, err := cache2.Get(ctx, i)
		if err != nil {
			t.Fatalf("Get[%d] from second cache: %v", i, err)
		}
		if !found {
			t.Errorf("expected key %d to be persisted", i)
		}
		if found && val != i*2 {
			t.Errorf("key %d: expected %d, got %d", i, i*2, val)
		}
	}
}

func TestCache_SetAsync_WithDefaultTTL(t *testing.T) {
	ctx := context.Background()
	cache, err := New[string, int](ctx, WithDefaultTTL(100*time.Millisecond))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// SetAsync with 0 TTL should use default
	if err := cache.SetAsync(ctx, "key1", 42, 0); err != nil {
		t.Fatalf("SetAsync: %v", err)
	}

	// Should be available immediately
	val, found, err := cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || val != 42 {
		t.Fatal("expected key1 to be found with value 42")
	}

	// Wait for default TTL to expire
	time.Sleep(150 * time.Millisecond)

	// Should be expired
	_, found, err = cache.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("Get after expiry: %v", err)
	}
	if found {
		t.Error("expected key1 to be expired after default TTL")
	}
}

func TestCache_SetAsync_ConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	cache, err := New[int, int](ctx)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := cache.Close(); err != nil {
			t.Logf("Close error: %v", err)
		}
	}()

	// Write same key concurrently
	const goroutines = 100
	done := make(chan bool, goroutines)

	for i := range goroutines {
		go func(val int) {
			if err := cache.SetAsync(ctx, 1, val, 0); err != nil {
				t.Logf("SetAsync error: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all to complete
	for range goroutines {
		<-done
	}

	// Key should exist with some value
	_, found, err := cache.Get(ctx, 1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found {
		t.Error("expected key 1 to exist after concurrent writes")
	}
}
