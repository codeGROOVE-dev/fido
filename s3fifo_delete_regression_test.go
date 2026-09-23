package fido

import "testing"

// A warm entry evicted from small remains addressable on death row, but is
// no longer linked into either FIFO. Deleting it must not unlink it again.
func TestS3FIFO_DeletePendingEviction(t *testing.T) {
	for _, fromSmall := range []bool{true, false} {
		name := "main"
		if fromSmall {
			name = "small"
		}
		t.Run(name, func(t *testing.T) {
			c := newS3FIFO[int, int](&config{size: 8})
			for i := 0; i < 8; i++ {
				c.set(i, i, 0)
			}
			e, _ := c.getEntry(0)
			e.setFreqPeak(0, 1)
			if !fromSmall {
				c.small.remove(e)
				e.setInSmall(false)
				c.main.pushBack(e)
				c.main.remove(e)
			} else {
				c.small.remove(e)
			}
			c.sendToDeathRow(e)
			if !e.onDeathRow() {
				t.Fatal("fixture did not enter pending eviction")
			}
			before := c.len()
			c.del(0)
			if c.len() != before {
				t.Fatalf("deleting pending entry changed live length: got %d want %d", c.len(), before)
			}
			for _, pending := range c.deathRow {
				if pending == e {
					t.Fatal("deleted entry remains in death row")
				}
			}
			if _, ok := c.getEntry(0); ok {
				t.Fatal("deleted entry remains addressable")
			}
			if c.small.head == nil || c.small.len != 7 {
				t.Fatal("deleting pending entry corrupted FIFO")
			}
			// Ring reuse must never erase a replacement inserted under the same key.
			c.set(0, 99, 0)
			for i := 8; i < 80; i++ {
				c.set(i, i, 0)
				c.get(i)
			}
		})
	}
}

func TestS3FIFO_InsertionPanicReleasesLock(t *testing.T) {
	c := newS3FIFO[int, int](&config{size: 8})
	c.hasher = func(int) uint64 { panic("injected hasher failure") }
	func() {
		defer func() {
			if recover() == nil {
				t.Error("expected panic")
			}
		}()
		c.setWithHash(1, 1, 0, 0)
	}()
	if !c.mu.TryLock() {
		t.Fatal("recovered insertion panic left writer lock held")
	}
	c.mu.Unlock()
}

// Reproduce the production failure using public operations only: retain a
// once-read entry during eviction, invalidate it, then keep inserting.
func TestCacheDeleteAfterEviction(t *testing.T) {
	c := New[int, int](Size(8))
	for i := 0; i < 8; i++ {
		c.Set(i, i)
	}
	c.Get(0)
	c.Set(8, 8)
	c.Delete(0)
	if c.Len() != 8 {
		t.Fatalf("deleting an evicted entry changed live count to %d", c.Len())
	}
	for i := 9; i < 100; i++ {
		c.Set(i, i)
	}
	if got, ok := c.Get(99); !ok || got != 99 {
		t.Fatalf("cache stopped after invalidation: %d %v", got, ok)
	}
}
