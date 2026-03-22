package cache

import (
	"container/list"
	"sync"
)

// LRUCache LRU缓存实现
type LRUCache struct {
	mu       sync.RWMutex
	capacity int
	cache    map[string]*list.Element
	list     *list.List
	stats    *CacheStats
}

// Entry 缓存条目
type Entry struct {
	key   string
	value string
}

// CacheStats 缓存统计
type CacheStats struct {
	Hits      int64
	Misses    int64
	Evictions int64
}

// NewLRUCache 创建新的LRU缓存
func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
		stats:    &CacheStats{},
	}
}

// Get 获取缓存值
func (lru *LRUCache) Get(key string) (string, bool) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	elem, exists := lru.cache[key]
	if !exists {
		lru.stats.Misses++
		return "", false
	}

	lru.list.MoveToFront(elem)
	lru.stats.Hits++
	return elem.Value.(*Entry).value, true
}

// Put 设置缓存值
func (lru *LRUCache) Put(key, value string) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.cache[key]; exists {
		elem.Value.(*Entry).value = value
		lru.list.MoveToFront(elem)
		return
	}

	entry := &Entry{key: key, value: value}
	elem := lru.list.PushFront(entry)
	lru.cache[key] = elem

	if lru.list.Len() > lru.capacity {
		oldest := lru.list.Back()
		lru.list.Remove(oldest)
		delete(lru.cache, oldest.Value.(*Entry).key)
		lru.stats.Evictions++
	}
}

// Delete 删除缓存值
func (lru *LRUCache) Delete(key string) bool {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if elem, exists := lru.cache[key]; exists {
		lru.list.Remove(elem)
		delete(lru.cache, key)
		return true
	}
	return false
}

// Clear 清空缓存
func (lru *LRUCache) Clear() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.cache = make(map[string]*list.Element)
	lru.list = list.New()
}

// GetStats 获取缓存统计信息
func (lru *LRUCache) GetStats() CacheStats {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	return CacheStats{
		Hits:      lru.stats.Hits,
		Misses:    lru.stats.Misses,
		Evictions: lru.stats.Evictions,
	}
}

// HitRate 获取缓存命中率
func (lru *LRUCache) HitRate() float64 {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	total := lru.stats.Hits + lru.stats.Misses
	if total == 0 {
		return 0
	}
	return float64(lru.stats.Hits) / float64(total)
}

// Size 获取当前缓存大小
func (lru *LRUCache) Size() int {
	lru.mu.RLock()
	defer lru.mu.RUnlock()

	return len(lru.cache)
}

// Capacity 获取缓存容量
func (lru *LRUCache) Capacity() int {
	return lru.capacity
}
