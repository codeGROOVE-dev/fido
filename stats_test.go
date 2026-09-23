package fido

import "testing"

func TestMemoryStats(t *testing.T) {
	c := New[string, []byte](Size(8))
	sizeOf := func(k string, v []byte) uint64 { return uint64(len(k) + cap(v)) }
	c.Set("abc", make([]byte, 2, 10))
	st, ok := c.MemoryStats(sizeOf)
	if !ok || st.Entries != 1 || st.RetainedEntries != 1 || st.Capacity != 8 || st.PayloadBytes != 13 || st.StructuralBytes == 0 {
		t.Fatalf("unexpected stats: %+v, %v", st, ok)
	}
	c.memory.mu.Lock()
	_, ok = c.MemoryStats(sizeOf)
	c.memory.mu.Unlock()
	if ok {
		t.Fatal("snapshot should report busy without waiting")
	}
	c.Delete("abc")
	st, ok = c.MemoryStats(sizeOf)
	if !ok || st.Entries != 0 || st.PayloadBytes != 0 {
		t.Fatalf("deleted payload retained: %+v", st)
	}
	c.Set("expired", make([]byte, 5))
	e, _ := c.memory.getEntry("expired")
	e.expirySec.Store(1)
	st, ok = c.MemoryStats(sizeOf)
	if !ok || st.PayloadBytes != 12 {
		t.Fatalf("expired storage not counted: %+v", st)
	}
}

func TestMemoryStatsPendingEviction(t *testing.T) {
	c := New[string, []byte](Size(8))
	c.Set("a", make([]byte, 10))
	e, _ := c.memory.getEntry("a")
	e.setFreqPeak(0, 1)
	c.memory.small.remove(e)
	c.memory.sendToDeathRow(e)
	st, ok := c.MemoryStats(func(k string, v []byte) uint64 { return uint64(len(k) + cap(v)) })
	if !ok || st.Entries != 0 || st.PendingEntries != 1 || st.RetainedEntries != 1 || st.PayloadBytes != 11 {
		t.Fatalf("pending storage not counted: %+v", st)
	}
}

func TestMemoryStatsSizerDoesNotHoldWriterLock(t *testing.T) {
	c := New[int, int](Size(8))
	c.Set(1, 2)
	_, ok := c.MemoryStats(func(int, int) uint64 {
		if !c.memory.mu.TryLock() {
			t.Fatal("sizer runs under writer lock")
		}
		c.memory.mu.Unlock()
		return 1
	})
	if !ok {
		t.Fatal("unexpected busy cache")
	}
}

func BenchmarkMemoryStats128K(b *testing.B) {
	c := New[int, []byte](Size(128000))
	for i := 0; i < 128000; i++ {
		c.Set(i, make([]byte, 256))
	}
	sizeOf := func(_ int, v []byte) uint64 { return uint64(cap(v)) }
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		c.MemoryStats(sizeOf)
	}
}
