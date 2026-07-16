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
	if !changed || healthy {
		t.Fatalf("second failure should mark unhealthy, changed=%v healthy=%v", changed, healthy)
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

	owner, gotPrimary, failover := m.WriteOwner(key, 2)
	if gotPrimary != primary {
		t.Fatalf("expected primary %s, got %s", primary, gotPrimary)
	}
	if !failover || owner == primary {
		t.Fatalf("expected failover away from %s, got owner=%s failover=%v", primary, owner, failover)
	}
}
