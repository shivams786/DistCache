package hash

import (
	"fmt"
	"testing"
)

func TestStableOwner(t *testing.T) {
	r := NewRing([]string{"node-1", "node-2", "node-3"}, 100)
	first, ok := r.Owner("user:101")
	if !ok {
		t.Fatal("expected owner")
	}
	for i := 0; i < 100; i++ {
		got, ok := r.Owner("user:101")
		if !ok || got != first {
			t.Fatalf("owner changed: %q -> %q", first, got)
		}
	}
}

func TestDistribution(t *testing.T) {
	r := NewRing([]string{"node-1", "node-2", "node-3"}, 200)
	counts := map[string]int{}
	for i := 0; i < 10000; i++ {
		owner, ok := r.Owner(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("expected owner")
		}
		counts[owner]++
	}
	for node, count := range counts {
		if count > 4500 {
			t.Fatalf("node %s has skewed distribution: %d", node, count)
		}
	}
}

func TestRemovalRemapsSubset(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}
	r := NewRing(nodes, 100)
	before := map[string]string{}
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key], _ = r.Owner(key)
	}

	r.RemoveNode("node-2")
	remapped := 0
	for key, owner := range before {
		next, _ := r.Owner(key)
		if next != owner {
			remapped++
		}
	}
	if remapped == 0 || remapped >= 4500 {
		t.Fatalf("unexpected remap count after removal: %d", remapped)
	}
}

func TestReplicasDiffer(t *testing.T) {
	r := NewRing([]string{"node-1", "node-2", "node-3"}, 100)
	owners := r.Owners("user:101", 2)
	if len(owners) != 2 {
		t.Fatalf("expected two owners, got %v", owners)
	}
	if owners[0] == owners[1] {
		t.Fatalf("replica should differ from primary: %v", owners)
	}
}

func TestAddingRemovedNodeBackProducesValidRing(t *testing.T) {
	r := NewRing([]string{"node-1", "node-2", "node-3"}, 100)
	r.RemoveNode("node-2")
	r.AddNode("node-2")

	nodes := r.Nodes()
	if len(nodes) != 3 {
		t.Fatalf("expected three nodes after add back, got %v", nodes)
	}
	for i := 0; i < 10000; i++ {
		owners := r.Owners(fmt.Sprintf("key-%d", i), 2)
		if len(owners) != 2 || owners[0] == owners[1] {
			t.Fatalf("invalid owners after add back for key %d: %v", i, owners)
		}
	}
}

func TestReplicationFactorCappedByNodeCount(t *testing.T) {
	r := NewRing([]string{"node-1", "node-2"}, 10)
	owners := r.Owners("key", 5)
	if len(owners) != 2 {
		t.Fatalf("expected owners capped at 2, got %v", owners)
	}
}

func TestEmptyRingHasNoOwners(t *testing.T) {
	r := NewRing(nil, 100)
	if _, ok := r.Owner("key"); ok {
		t.Fatal("empty ring should not have an owner")
	}
	if owners := r.Owners("key", 2); len(owners) != 0 {
		t.Fatalf("empty ring owners = %v", owners)
	}
}

func TestConcurrentRingOwners(t *testing.T) {
	r := NewRing([]string{"node-1", "node-2", "node-3"}, 100)
	done := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		go func(id int) {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 1000; i++ {
				owners := r.Owners(fmt.Sprintf("worker-%d-key-%d", id, i), 2)
				if len(owners) != 2 || owners[0] == owners[1] {
					t.Errorf("invalid owners: %v", owners)
					return
				}
			}
		}(worker)
	}
	for worker := 0; worker < 8; worker++ {
		<-done
	}
}
