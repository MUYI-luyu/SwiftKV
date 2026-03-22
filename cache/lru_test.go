package cache

import (
	"sync"
	"testing"
)

// TestLRUBasicGet tests basic Get operation
func TestLRUBasicGet(t *testing.T) {
	lru := NewLRUCache(10)

	// Get from empty cache should return false
	val, ok := lru.Get("key1")
	if ok {
		t.Errorf("Expected false for missing key, got true")
	}

	// Put then Get should work
	lru.Put("key1", "value1")
	val, ok = lru.Get("key1")
	if !ok {
		t.Errorf("Expected true for existing key, got false")
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

// TestLRUPut tests Put operations
func TestLRUPut(t *testing.T) {
	lru := NewLRUCache(3)

	lru.Put("key1", "value1")
	lru.Put("key2", "value2")
	lru.Put("key3", "value3")

	// All should be retrievable
	for i := 1; i <= 3; i++ {
		key := "key" + string(rune('0'+i))
		_, ok := lru.Get(key)
		if !ok {
			t.Errorf("Expected to find %s", key)
		}
	}
}

// TestLRUEviction tests LRU eviction policy
func TestLRUEviction(t *testing.T) {
	lru := NewLRUCache(3)

	lru.Put("key1", "value1")
	lru.Put("key2", "value2")
	lru.Put("key3", "value3")

	// Access key1 to make it recently used
	lru.Get("key1")

	// Add key4, should evict key2 (least recently used)
	lru.Put("key4", "value4")

	// key2 should be evicted
	_, ok := lru.Get("key2")
	if ok {
		t.Errorf("Expected key2 to be evicted, but it still exists")
	}

	// key1 should still exist
	_, ok = lru.Get("key1")
	if !ok {
		t.Errorf("Expected key1 to exist")
	}

	// key3 and key4 should exist
	_, ok = lru.Get("key3")
	if !ok {
		t.Errorf("Expected key3 to exist")
	}
	_, ok = lru.Get("key4")
	if !ok {
		t.Errorf("Expected key4 to exist")
	}
}

// TestLRUDelete tests Delete operation
func TestLRUDelete(t *testing.T) {
	lru := NewLRUCache(10)

	lru.Put("key1", "value1")
	lru.Put("key2", "value2")

	lru.Delete("key1")

	_, ok := lru.Get("key1")
	if ok {
		t.Errorf("Expected key1 to be deleted, but it still exists")
	}

	// key2 should still exist
	_, ok = lru.Get("key2")
	if !ok {
		t.Errorf("Expected key2 to still exist")
	}
}

// TestLRUHitRate tests hit rate calculation
func TestLRUHitRate(t *testing.T) {
	lru := NewLRUCache(10)

	lru.Put("key1", "value1")

	// First hit
	lru.Get("key1")
	// First miss
	lru.Get("nonexistent")
	// Second hit
	lru.Get("key1")

	hitRate := lru.HitRate()
	expectedRate := float64(2) / float64(3) // 2 hits out of 3 accesses
	if hitRate != expectedRate {
		t.Errorf("Expected hit rate %f, got %f", expectedRate, hitRate)
	}
}

// TestLRUGetStats tests statistics collection
func TestLRUGetStats(t *testing.T) {
	lru := NewLRUCache(10)

	lru.Put("key1", "value1")
	lru.Put("key2", "value2")

	lru.Get("key1")        // hit
	lru.Get("key1")        // hit
	lru.Get("nonexistent") // miss

	stats := lru.GetStats()
	if stats.Hits != 2 {
		t.Errorf("Expected 2 hits, got %d", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Expected 1 miss, got %d", stats.Misses)
	}
}

// TestLRUUpdateValue tests updating existing key
func TestLRUUpdateValue(t *testing.T) {
	lru := NewLRUCache(10)

	lru.Put("key1", "value1")
	lru.Put("key1", "value2")

	val, ok := lru.Get("key1")
	if !ok {
		t.Errorf("Expected to find key1")
	}
	if val != "value2" {
		t.Errorf("Expected updated value2, got %s", val)
	}
}

// TestLRUClear tests Clear operation
func TestLRUClear(t *testing.T) {
	lru := NewLRUCache(10)

	lru.Put("key1", "value1")
	lru.Put("key2", "value2")

	lru.Clear()

	_, ok := lru.Get("key1")
	if ok {
		t.Errorf("Expected key1 to be cleared")
	}

	_, ok = lru.Get("key2")
	if ok {
		t.Errorf("Expected key2 to be cleared")
	}
}

// TestLRUConcurrentAccess tests thread-safe operations
func TestLRUConcurrentAccess(t *testing.T) {
	lru := NewLRUCache(100)

	var wg sync.WaitGroup
	numGoroutines := 10
	operationsPerGoroutine := 100

	// Concurrent writes
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				key := "key" + string(rune('0'+(i%10)))
				value := "value" + string(rune('0'+id))
				lru.Put(key, value)
			}
		}(g)
	}

	// Concurrent reads
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				key := "key" + string(rune('0'+(i%10)))
				lru.Get(key)
			}
		}(g)
	}

	// Concurrent deletes
	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine/2; i++ {
				key := "key" + string(rune('0'+(i%10)))
				lru.Delete(key)
			}
		}(g)
	}

	wg.Wait()

	// Verify no panic occurred and stats are reasonable
	stats := lru.GetStats()
	if stats.Hits < 0 || stats.Misses < 0 {
		t.Errorf("Invalid stats after concurrent access: Hits=%d, Misses=%d",
			stats.Hits, stats.Misses)
	}
}

// BenchmarkLRUGet benchmarks Get operation
func BenchmarkLRUGet(b *testing.B) {
	lru := NewLRUCache(10000)

	// Populate cache
	for i := 0; i < 1000; i++ {
		key := "key" + string(rune('0'+(i%10)))
		lru.Put(key, "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + string(rune('0'+(i%10)))
		lru.Get(key)
	}
}

// BenchmarkLRUPut benchmarks Put operation
func BenchmarkLRUPut(b *testing.B) {
	lru := NewLRUCache(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + string(rune('0'+(i%1000)))
		lru.Put(key, "value")
	}
}

// BenchmarkLRUDeleteEviction benchmarks deletion due to LRU eviction
func BenchmarkLRUDeleteEviction(b *testing.B) {
	lru := NewLRUCache(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + string(rune('0'+(i%1000)))
		lru.Put(key, "value")
	}
}
