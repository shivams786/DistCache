package hash

import "testing"

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
	for i := 0; i < 3000; i++ {
		owner, ok := r.Owner(string(rune(i)) + "-key")
		if !ok {
			t.Fatal("expected owner")
		}
		counts[owner]++
	}
	for node, count := range counts {
		if count < 700 || count > 1300 {
			t.Fatalf("node %s has skewed distribution: %d", node, count)
		}
	}
}

func TestRemovalRemapsSubset(t *testing.T) {
	nodes := []string{"node-1", "node-2", "node-3"}
	r := NewRing(nodes, 100)
	before := map[string]string{}
	for i := 0; i < 1000; i++ {
		key := string(rune(i)) + "-key"
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
	if remapped == 0 || remapped > 600 {
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
