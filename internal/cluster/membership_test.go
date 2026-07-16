package cluster

import "testing"

func TestHealthTransitionsRequireMultipleChecks(t *testing.T) {
	m := NewMembership("node-1", []Node{
		{ID: "node-1", GRPCAddress: "node-1:9090"},
		{ID: "node-2", GRPCAddress: "node-2:9090"},
	}, 10)

	changed, healthy := m.RecordHealth("node-2", false, 0, 0)
	if changed || !healthy {
		t.Fatalf("first failure should not mark unhealthy, changed=%v healthy=%v", changed, healthy)
	}
	changed, healthy = m.RecordHealth("node-2", false, 0, 0)
	if changed || !healthy {
		t.Fatalf("second failure should not mark unhealthy, changed=%v healthy=%v", changed, healthy)
	}
	changed, healthy = m.RecordHealth("node-2", false, 0, 0)
	if !changed || healthy {
		t.Fatalf("third failure should mark unhealthy, changed=%v healthy=%v", changed, healthy)
	}
	changed, healthy = m.RecordHealth("node-2", true, 1, 10)
	if changed || healthy {
		t.Fatalf("first recovery success should not mark healthy, changed=%v healthy=%v", changed, healthy)
	}
	changed, healthy = m.RecordHealth("node-2", true, 1, 10)
	if !changed || !healthy {
		t.Fatalf("second recovery success should mark healthy, changed=%v healthy=%v", changed, healthy)
	}
}

func TestWriteOwnerFailsOver(t *testing.T) {
	m := NewMembership("node-1", []Node{
		{ID: "node-1", GRPCAddress: "node-1:9090"},
		{ID: "node-2", GRPCAddress: "node-2:9090"},
		{ID: "node-3", GRPCAddress: "node-3:9090"},
	}, 100)
	key := "user:101"
	_, primary, _ := m.WriteOwner(key, 2)
	m.RecordHealth(primary, false, 0, 0)
	m.RecordHealth(primary, false, 0, 0)
	m.RecordHealth(primary, false, 0, 0)

	owner, gotPrimary, failover := m.WriteOwner(key, 2)
	if gotPrimary != primary {
		t.Fatalf("expected primary %s, got %s", primary, gotPrimary)
	}
	if !failover || owner == primary {
		t.Fatalf("expected failover away from %s, got owner=%s failover=%v", primary, owner, failover)
	}
}

func TestParseNodesSupportsExplicitAndImplicitIDs(t *testing.T) {
	nodes, err := ParseNodes("cache-node-1=127.0.0.1:9091,cache-node-2:9090")
	if err != nil {
		t.Fatalf("parse nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0].ID != "cache-node-1" || nodes[0].GRPCAddress != "127.0.0.1:9091" {
		t.Fatalf("unexpected explicit node: %+v", nodes[0])
	}
	if nodes[1].ID != "cache-node-2" || nodes[1].GRPCAddress != "cache-node-2:9090" {
		t.Fatalf("unexpected implicit node: %+v", nodes[1])
	}
}

func TestReplicaForSkipsUnhealthyNode(t *testing.T) {
	m := NewMembership("node-1", []Node{
		{ID: "node-1", GRPCAddress: "node-1:9090"},
		{ID: "node-2", GRPCAddress: "node-2:9090"},
		{ID: "node-3", GRPCAddress: "node-3:9090"},
	}, 100)
	key := "replica-skip"
	replica, ok := m.ReplicaFor(key, "node-1")
	if !ok {
		t.Fatal("expected replica")
	}
	m.RecordHealth(replica, false, 0, 0)
	m.RecordHealth(replica, false, 0, 0)
	m.RecordHealth(replica, false, 0, 0)
	next, ok := m.ReplicaFor(key, "node-1")
	if !ok || next == replica || next == "node-1" {
		t.Fatalf("expected healthy alternate replica, got %q original %q", next, replica)
	}
}

func TestMarkSelfUpdatesLocalStatus(t *testing.T) {
	m := NewMembership("node-1", []Node{{ID: "node-1", GRPCAddress: "node-1:9090"}}, 10)
	m.MarkSelf(42, 99)
	status := m.Snapshot()[0]
	if !status.Healthy || status.CacheEntries != 42 || status.RequestCount != 99 {
		t.Fatalf("unexpected self status: %+v", status)
	}
}
