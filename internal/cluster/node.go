package cluster

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/codex/distcache/internal/hash"
)

type Node struct {
	ID          string `json:"id"`
	GRPCAddress string `json:"grpc_address"`
}

type NodeStatus struct {
	Node                Node      `json:"node"`
	Healthy            bool      `json:"healthy"`
	LastSuccessfulCheck time.Time `json:"last_successful_check,omitempty"`
	ConsecutiveSuccess  int       `json:"consecutive_success"`
	ConsecutiveFailure  int       `json:"consecutive_failure"`
	CacheEntries        int64     `json:"cache_entries"`
	RequestCount        uint64    `json:"request_count"`
}

func ParseNodes(raw string) ([]Node, error) {
	parts := strings.Split(raw, ",")
	nodes := make([]Node, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		var id, address string
		if left, right, ok := strings.Cut(part, "="); ok {
			id = strings.TrimSpace(left)
			address = strings.TrimSpace(right)
		} else {
			address = part
			host := part
			if strings.Contains(part, ":") {
				host = part[:strings.LastIndex(part, ":")]
			}
			id = strings.TrimSpace(host)
		}
		if id == "" || address == "" {
			return nil, fmt.Errorf("invalid cluster node %q", part)
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		nodes = append(nodes, Node{ID: id, GRPCAddress: address})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes, nil
}

type Membership struct {
	mu               sync.RWMutex
	selfID           string
	statuses         map[string]*NodeStatus
	ring             *hash.Ring
	healthyAfter     int
	unhealthyAfter   int
}

func NewMembership(selfID string, nodes []Node, virtualNodes int) *Membership {
	return NewMembershipWithThresholds(selfID, nodes, virtualNodes, 3, 2)
}

func NewMembershipWithThresholds(selfID string, nodes []Node, virtualNodes int, unhealthyAfter int, healthyAfter int) *Membership {
	if unhealthyAfter <= 0 {
		unhealthyAfter = 3
	}
	if healthyAfter <= 0 {
		healthyAfter = 2
	}
	statuses := make(map[string]*NodeStatus, len(nodes))
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		healthy := true
		statuses[node.ID] = &NodeStatus{
			Node:                node,
			Healthy:            healthy,
			LastSuccessfulCheck: time.Now().UTC(),
		}
		ids = append(ids, node.ID)
	}
	if _, ok := statuses[selfID]; !ok {
		node := Node{ID: selfID, GRPCAddress: selfID}
		statuses[selfID] = &NodeStatus{Node: node, Healthy: true, LastSuccessfulCheck: time.Now().UTC()}
		ids = append(ids, selfID)
	}
	return &Membership{
		selfID:         selfID,
		statuses:       statuses,
		ring:           hash.NewRing(ids, virtualNodes),
		healthyAfter:   healthyAfter,
		unhealthyAfter: unhealthyAfter,
	}
}

func (m *Membership) Node(id string) (Node, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status, ok := m.statuses[id]
	if !ok {
		return Node{}, false
	}
	return status.Node, true
}

func (m *Membership) Nodes() []Node {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]Node, 0, len(m.statuses))
	for _, status := range m.statuses {
		nodes = append(nodes, status.Node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func (m *Membership) Snapshot() []NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	statuses := make([]NodeStatus, 0, len(m.statuses))
	for _, status := range m.statuses {
		statuses = append(statuses, *status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Node.ID < statuses[j].Node.ID
	})
	return statuses
}

func (m *Membership) MarkSelf(entries int, requests uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status := m.statuses[m.selfID]
	status.Healthy = true
	status.ConsecutiveSuccess++
	status.ConsecutiveFailure = 0
	status.CacheEntries = int64(entries)
	status.RequestCount = requests
	status.LastSuccessfulCheck = time.Now().UTC()
}

func (m *Membership) RecordHealth(id string, ok bool, entries int64, requests uint64) (changed bool, healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	status, exists := m.statuses[id]
	if !exists {
		return false, false
	}
	previous := status.Healthy
	if ok {
		status.ConsecutiveSuccess++
		status.ConsecutiveFailure = 0
		status.CacheEntries = entries
		status.RequestCount = requests
		status.LastSuccessfulCheck = time.Now().UTC()
		if !status.Healthy && status.ConsecutiveSuccess >= m.healthyAfter {
			status.Healthy = true
		}
	} else {
		status.ConsecutiveFailure++
		status.ConsecutiveSuccess = 0
		if status.Healthy && status.ConsecutiveFailure >= m.unhealthyAfter {
			status.Healthy = false
		}
	}
	return previous != status.Healthy, status.Healthy
}

func (m *Membership) Healthy(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status, ok := m.statuses[id]
	return ok && status.Healthy
}

func (m *Membership) Owners(key string, replicationFactor int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ring.Owners(key, replicationFactor)
}

func (m *Membership) AllOwners(key string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ring.Owners(key, len(m.statuses))
}

func (m *Membership) WriteOwner(key string, replicationFactor int) (owner string, primary string, failover bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	owners := m.ring.Owners(key, len(m.statuses))
	if len(owners) == 0 {
		return "", "", false
	}
	primary = owners[0]
	if m.healthyLocked(primary) {
		return primary, primary, false
	}
	for _, node := range owners[1:] {
		if m.healthyLocked(node) {
			return node, primary, true
		}
	}
	return primary, primary, true
}

func (m *Membership) ReadOwners(key string, replicationFactor int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	owners := m.ring.Owners(key, replicationFactor)
	if len(owners) == 0 {
		return nil
	}
	healthy := make([]string, 0, len(owners))
	for _, owner := range owners {
		if m.healthyLocked(owner) {
			healthy = append(healthy, owner)
		}
	}
	if len(healthy) > 0 {
		return healthy
	}
	return owners
}

func (m *Membership) ReplicaFor(key, exclude string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	owners := m.ring.Owners(key, len(m.statuses))
	for _, owner := range owners {
		if owner != exclude && m.healthyLocked(owner) {
			return owner, true
		}
	}
	return "", false
}

func (m *Membership) ShouldStoreOnNode(key, nodeID string, replicationFactor int) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, owner := range m.ring.Owners(key, replicationFactor) {
		if owner == nodeID {
			return true
		}
	}
	return false
}

func (m *Membership) healthyLocked(id string) bool {
	status, ok := m.statuses[id]
	return ok && status.Healthy
}
