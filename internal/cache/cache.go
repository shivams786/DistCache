package cache

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"
)

type Config struct {
	MaxEntries      int
	CleanupInterval time.Duration
	Logger          *slog.Logger
}

type Stats struct {
	Entries   int   `json:"entries"`
	Hits      int64 `json:"hits"`
	Misses    int64 `json:"misses"`
	Evictions int64 `json:"evictions"`
	Expired   int64 `json:"expired"`
}

type Cache struct {
	mu              sync.RWMutex
	items           map[string]*list.Element
	lru             *list.List
	maxEntries      int
	cleanupInterval time.Duration
	logger          *slog.Logger
	hits            int64
	misses          int64
	evictions       int64
	expired         int64
}

func New(cfg Config) *Cache {
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 10000
	}
	if cfg.CleanupInterval <= 0 {
		cfg.CleanupInterval = 5 * time.Second
	}
	return &Cache{
		items:           make(map[string]*list.Element),
		lru:             list.New(),
		maxEntries:      cfg.MaxEntries,
		cleanupInterval: cfg.CleanupInterval,
		logger:          cfg.Logger,
	}
}

func (c *Cache) Set(key string, value []byte, ttl time.Duration) Entry {
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().UTC().Add(ttl)
	}
	return c.SetWithExpiration(key, value, expiresAt)
}

func (c *Cache) SetWithExpiration(key string, value []byte, expiresAt time.Time) Entry {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*Entry)
		entry.Value = append(entry.Value[:0], value...)
		entry.ExpiresAt = expiresAt
		entry.AccessedAt = now
		c.lru.MoveToFront(elem)
		return entry.Clone()
	}

	c.removeExpiredLocked(now)
	for len(c.items) >= c.maxEntries {
		c.removeLRULocked()
	}

	entry := &Entry{
		Key:        key,
		Value:      append([]byte(nil), value...),
		ExpiresAt:  expiresAt,
		CreatedAt:  now,
		AccessedAt: now,
	}
	elem := c.lru.PushFront(entry)
	c.items[key] = elem
	return entry.Clone()
}

func (c *Cache) Get(key string) (Entry, bool) {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		c.misses++
		return Entry{}, false
	}

	entry := elem.Value.(*Entry)
	if entry.Expired(now) {
		c.removeElementLocked(elem, "expired")
		c.misses++
		return Entry{}, false
	}

	entry.AccessedAt = now
	entry.HitCount++
	c.hits++
	c.lru.MoveToFront(elem)
	return entry.Clone(), true
}

func (c *Cache) Delete(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return false
	}
	c.removeElementLocked(elem, "delete")
	return true
}

func (c *Cache) Exists(key string) bool {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[key]
	if !ok {
		return false
	}
	entry := elem.Value.(*Entry)
	if entry.Expired(now) {
		c.removeElementLocked(elem, "expired")
		return false
	}
	return true
}

func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.lru.Init()
}

func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return Stats{
		Entries:   len(c.items),
		Hits:      c.hits,
		Misses:    c.misses,
		Evictions: c.evictions,
		Expired:   c.expired,
	}
}

func (c *Cache) Snapshot() []Entry {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()

	c.removeExpiredLocked(now)
	entries := make([]Entry, 0, len(c.items))
	for elem := c.lru.Front(); elem != nil; elem = elem.Next() {
		entries = append(entries, elem.Value.(*Entry).Clone())
	}
	return entries
}

func (c *Cache) CleanupExpired() int {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.removeExpiredLocked(now)
}

func (c *Cache) StartCleanup(ctx context.Context) {
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.CleanupExpired()
		}
	}
}

func (c *Cache) removeExpiredLocked(now time.Time) int {
	removed := 0
	for key, elem := range c.items {
		entry := elem.Value.(*Entry)
		if entry.Expired(now) {
			delete(c.items, key)
			c.lru.Remove(elem)
			c.expired++
			removed++
			c.log("entry_expired", entry.Key, "ttl")
		}
	}
	return removed
}

func (c *Cache) removeLRULocked() {
	elem := c.lru.Back()
	if elem == nil {
		return
	}
	c.removeElementLocked(elem, "capacity")
}

func (c *Cache) removeElementLocked(elem *list.Element, reason string) {
	entry := elem.Value.(*Entry)
	delete(c.items, entry.Key)
	c.lru.Remove(elem)
	switch reason {
	case "capacity":
		c.evictions++
		c.log("entry_evicted", entry.Key, reason)
	case "expired":
		c.expired++
		c.log("entry_expired", entry.Key, reason)
	}
}

func (c *Cache) log(event, key, reason string) {
	if c.logger == nil {
		return
	}
	c.logger.Info("cache entry removed",
		"event", event,
		"key_hash", KeyHash(key),
		"reason", reason,
	)
}

func KeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:8])
}
