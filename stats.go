package fido

import "unsafe"

// MemoryStats describes retained memory, including expired entries and entries
// awaiting eviction. Counts and bytes are estimates during concurrent updates.
// Bytes excludes allocator rounding, goroutine stacks, in-flight loads, and the
// internal allocations of xsync maps and locks. It is not process RSS.
type MemoryStats struct {
	Entries         int
	Capacity        int
	PendingEntries  int
	RetainedEntries int
	StructuralBytes uint64
	PayloadBytes    uint64
}

// Bytes returns the estimated bytes accounted for by this snapshot.
func (s MemoryStats) Bytes() uint64 { return s.StructuralBytes + s.PayloadBytes }

// MemoryStats samples the cache without waiting for its writer lock. False
// means the cache is busy; retain the previous sample and retry later. With a
// non-nil sizeOf it takes O(N) time and should run periodically, not per request.
// sizeOf reports referenced key/value storage beyond their inline Go headers;
// for string keys and []byte values this is len(key)+cap(value). It must be fast,
// must not mutate the cache or its values. Shared backing storage
// can be counted multiple times. A nil sizeOf reports only structural bytes.
func (c *Cache[K, V]) MemoryStats(sizeOf func(K, V) uint64) (MemoryStats, bool) {
	return c.memory.memoryStats(sizeOf)
}

// MemoryStats samples only the memory tier. Persistent storage is owned by
// Store and is deliberately not scanned or contacted by this method.
func (c *TieredCache[K, V]) MemoryStats(sizeOf func(K, V) uint64) (MemoryStats, bool) {
	return c.memory.memoryStats(sizeOf)
}

func (c *s3fifo[K, V]) memoryStats(sizeOf func(K, V) uint64) (MemoryStats, bool) {
	var s MemoryStats
	var recycledKey K
	var recycledValue V
	var recycled bool
	// Only queue metadata needs the writer lock. Traverse the concurrent map
	// and call user code after releasing it, like Range; diagnostics must not
	// stop insertions for the duration of a full-cache scan.
	if !func() bool {
		if !c.mu.TryLock() {
			return false
		}
		defer c.mu.Unlock()
		s = MemoryStats{Entries: c.len(), Capacity: c.capacity, RetainedEntries: c.entries.Size()}
		for _, e := range c.deathRow {
			if e != nil {
				s.PendingEntries++
			}
		}
		var e entry[K, V]
		s.StructuralBytes = uint64(unsafe.Sizeof(*c)) + 2*uint64(unsafe.Sizeof(bloomFilter{})) +
			uint64(cap(c.ghostActive.data)+cap(c.ghostAging.data))*8 +
			uint64(cap(c.deathRow))*uint64(unsafe.Sizeof(&e)) +
			uint64(s.RetainedEntries)*uint64(unsafe.Sizeof(e))
		if c.freeEntry != nil {
			s.StructuralBytes += uint64(unsafe.Sizeof(e))
			if sizeOf != nil {
				recycledKey = c.freeEntry.key
				recycledValue, recycled = c.freeEntry.loadValue()
			}
		}
		return true
	}() {
		return MemoryStats{}, false
	}
	if sizeOf != nil {
		c.entries.Range(func(k K, e *entry[K, V]) bool {
			if v, ok := e.loadValue(); ok {
				s.PayloadBytes += sizeOf(k, v)
			}
			return true
		})
		if recycled {
			s.PayloadBytes += sizeOf(recycledKey, recycledValue)
		}
	}
	return s, true
}
