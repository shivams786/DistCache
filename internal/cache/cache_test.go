package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSetGetDelete(t *testing.T) {
	c := New(Config{MaxEntries: 10, CleanupInterval: time.Hour})
	c.Set("alpha", []byte("one"), 0)

	entry, ok := c.Get("alpha")
	if !ok || string(entry.Value) != "one" {
		t.Fatalf("expected cached value, got ok=%v value=%q", ok, entry.Value)
	}
	if !c.Delete("alpha") {
		t.Fatal("expected delete to report true")
	}
	if _, ok := c.Get("alpha"); ok {
		t.Fatal("expected value to be deleted")
	}
}

func TestTTLExpiration(t *testing.T) {
	c := New(Config{MaxEntries: 10, CleanupInterval: time.Hour})
	c.Set("short", []byte("lived"), 15*time.Millisecond)
	time.Sleep(30 * time.Millisecond)

	if _, ok := c.Get("short"); ok {
		t.Fatal("expected expired value to miss")
	}
	if c.Stats().Expired == 0 {
		t.Fatal("expected expired counter to increase")
	}
}

func TestLRUEviction(t *testing.T) {
	c := New(Config{MaxEntries: 2, CleanupInterval: time.Hour})
	c.Set("a", []byte("1"), 0)
	c.Set("b", []byte("2"), 0)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected a to exist")
	}
	c.Set("c", []byte("3"), 0)

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected least recently used key b to be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected recently used key a to remain")
	}
}

func TestConcurrentAccess(t *testing.T) {
	c := New(Config{MaxEntries: 200, CleanupInterval: time.Hour})
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j%10)
				c.Set(key, []byte("value"), time.Minute)
				c.Get(key)
				c.Exists(key)
			}
		}(i)
	}
	wg.Wait()
}

func TestExistsAndClear(t *testing.T) {
	c := New(Config{MaxEntries: 10, CleanupInterval: time.Hour})
	c.Set("exists", []byte("yes"), time.Minute)
	if !c.Exists("exists") {
		t.Fatal("expected key to exist")
	}
	c.Clear()
	if c.Size() != 0 || c.Exists("exists") {
		t.Fatalf("expected clear to remove all entries, size=%d", c.Size())
	}
}

func TestCleanupExpiredRemovesOnlyExpiredEntries(t *testing.T) {
	c := New(Config{MaxEntries: 10, CleanupInterval: time.Hour})
	c.Set("expired", []byte("gone"), 10*time.Millisecond)
	c.Set("live", []byte("stay"), time.Minute)
	time.Sleep(20 * time.Millisecond)

	removed := c.CleanupExpired()
	if removed != 1 {
		t.Fatalf("expected one expired entry removed, got %d", removed)
	}
	if _, ok := c.Get("live"); !ok {
		t.Fatal("live key should remain")
	}
}

func TestSnapshotClonesValues(t *testing.T) {
	c := New(Config{MaxEntries: 10, CleanupInterval: time.Hour})
	c.Set("clone", []byte("before"), time.Minute)
	snapshot := c.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one snapshot entry, got %d", len(snapshot))
	}
	snapshot[0].Value[0] = 'x'
	entry, ok := c.Get("clone")
	if !ok || string(entry.Value) != "before" {
		t.Fatalf("cache value mutated through snapshot: ok=%v value=%q", ok, entry.Value)
	}
}

func TestExpiredEntriesRemovedBeforeLRUEviction(t *testing.T) {
	c := New(Config{MaxEntries: 2, CleanupInterval: time.Hour})
	c.Set("expired", []byte("gone"), 10*time.Millisecond)
	c.Set("live", []byte("stay"), time.Minute)
	time.Sleep(20 * time.Millisecond)
	c.Set("new", []byte("fresh"), time.Minute)

	if c.Exists("expired") {
		t.Fatal("expired key should be removed")
	}
	if !c.Exists("live") || !c.Exists("new") {
		t.Fatal("live and new keys should remain")
	}
}

func TestConcurrentSnapshotAndDelete(t *testing.T) {
	c := New(Config{MaxEntries: 1000, CleanupInterval: time.Hour})
	for i := 0; i < 500; i++ {
		c.Set(fmt.Sprintf("key-%d", i), []byte("value"), time.Minute)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				key := fmt.Sprintf("key-%d", (id+j)%500)
				c.Snapshot()
				c.Delete(key)
				c.Set(key, []byte("value"), time.Minute)
			}
		}(i)
	}
	wg.Wait()
}
