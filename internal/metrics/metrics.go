package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/codex/distcache/internal/cache"
)

type durationStats struct {
	Count int64
	Sum   float64
}

type Snapshot struct {
	Requests             map[string]int64 `json:"requests"`
	ReplicationSuccess   int64            `json:"replication_success"`
	ReplicationFailures  int64            `json:"replication_failures"`
	Failovers            int64            `json:"failovers"`
	RequestDurationCount int64            `json:"request_duration_count"`
	GRPCDurationCount    int64            `json:"grpc_duration_count"`
}

type Metrics struct {
	mu                    sync.RWMutex
	nodeID                string
	requests              map[string]int64
	requestDurations      map[string]*durationStats
	grpcDurations         map[string]*durationStats
	replicationSuccess    int64
	replicationFailures   int64
	failovers             int64
	nodeHealth            map[string]bool
	totalRequestDurations int64
	totalGRPCDurations    int64
}

func New(nodeID string) *Metrics {
	return &Metrics{
		nodeID:           nodeID,
		requests:         map[string]int64{},
		requestDurations: map[string]*durationStats{},
		grpcDurations:    map[string]*durationStats{},
		nodeHealth:       map[string]bool{},
	}
}

func (m *Metrics) IncRequest(operation, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[labelKey(operation, status)]++
}

func (m *Metrics) ObserveRequest(operation, status string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := labelKey(operation, status)
	stats := m.requestDurations[key]
	if stats == nil {
		stats = &durationStats{}
		m.requestDurations[key] = stats
	}
	stats.Count++
	stats.Sum += duration.Seconds()
	m.totalRequestDurations++
}

func (m *Metrics) ObserveGRPC(method, status string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := labelKey(method, status)
	stats := m.grpcDurations[key]
	if stats == nil {
		stats = &durationStats{}
		m.grpcDurations[key] = stats
	}
	stats.Count++
	stats.Sum += duration.Seconds()
	m.totalGRPCDurations++
}

func (m *Metrics) IncReplicationSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicationSuccess++
}

func (m *Metrics) IncReplicationFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicationFailures++
}

func (m *Metrics) IncFailover() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failovers++
}

func (m *Metrics) SetNodeHealth(node string, healthy bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodeHealth[node] = healthy
}

func (m *Metrics) RequestCount() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var total uint64
	for _, count := range m.requests {
		total += uint64(count)
	}
	return total
}

func (m *Metrics) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	requests := make(map[string]int64, len(m.requests))
	for key, value := range m.requests {
		requests[key] = value
	}
	return Snapshot{
		Requests:             requests,
		ReplicationSuccess:   m.replicationSuccess,
		ReplicationFailures:  m.replicationFailures,
		Failovers:            m.failovers,
		RequestDurationCount: m.totalRequestDurations,
		GRPCDurationCount:    m.totalGRPCDurations,
	}
}

func (m *Metrics) Render(cacheStats cache.Stats) string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var b strings.Builder
	b.WriteString("# HELP distcache_requests_total HTTP requests handled by this node.\n")
	b.WriteString("# TYPE distcache_requests_total counter\n")
	for _, key := range sortedKeys(m.requests) {
		operation, status := splitLabelKey(key)
		fmt.Fprintf(&b, "distcache_requests_total{operation=%q,status=%q,node=%q} %d\n", operation, status, m.nodeID, m.requests[key])
	}

	fmt.Fprintf(&b, "# HELP distcache_cache_hits_total Cache hits.\n# TYPE distcache_cache_hits_total counter\ndistcache_cache_hits_total{node=%q} %d\n", m.nodeID, cacheStats.Hits)
	fmt.Fprintf(&b, "# HELP distcache_cache_misses_total Cache misses.\n# TYPE distcache_cache_misses_total counter\ndistcache_cache_misses_total{node=%q} %d\n", m.nodeID, cacheStats.Misses)
	fmt.Fprintf(&b, "# HELP distcache_cache_entries In-memory cache entries.\n# TYPE distcache_cache_entries gauge\ndistcache_cache_entries{node=%q} %d\n", m.nodeID, cacheStats.Entries)
	fmt.Fprintf(&b, "# HELP distcache_evictions_total LRU capacity evictions.\n# TYPE distcache_evictions_total counter\ndistcache_evictions_total{node=%q} %d\n", m.nodeID, cacheStats.Evictions)
	fmt.Fprintf(&b, "# HELP distcache_expired_entries_total Expired entries removed.\n# TYPE distcache_expired_entries_total counter\ndistcache_expired_entries_total{node=%q} %d\n", m.nodeID, cacheStats.Expired)
	fmt.Fprintf(&b, "# HELP distcache_replication_success_total Successful replication attempts.\n# TYPE distcache_replication_success_total counter\ndistcache_replication_success_total{node=%q} %d\n", m.nodeID, m.replicationSuccess)
	fmt.Fprintf(&b, "# HELP distcache_replication_failures_total Failed replication attempts.\n# TYPE distcache_replication_failures_total counter\ndistcache_replication_failures_total{node=%q} %d\n", m.nodeID, m.replicationFailures)
	fmt.Fprintf(&b, "# HELP distcache_failovers_total Requests routed away from unhealthy primaries.\n# TYPE distcache_failovers_total counter\ndistcache_failovers_total{node=%q} %d\n", m.nodeID, m.failovers)

	b.WriteString("# HELP distcache_node_health Observed node health where 1 is healthy.\n")
	b.WriteString("# TYPE distcache_node_health gauge\n")
	for _, node := range sortedHealthKeys(m.nodeHealth) {
		value := 0
		if m.nodeHealth[node] {
			value = 1
		}
		fmt.Fprintf(&b, "distcache_node_health{node=%q,observed_by=%q} %d\n", node, m.nodeID, value)
	}

	b.WriteString("# HELP distcache_request_duration_seconds HTTP request duration summary.\n")
	b.WriteString("# TYPE distcache_request_duration_seconds summary\n")
	for _, key := range sortedDurationKeys(m.requestDurations) {
		operation, status := splitLabelKey(key)
		stats := m.requestDurations[key]
		fmt.Fprintf(&b, "distcache_request_duration_seconds_count{operation=%q,status=%q,node=%q} %d\n", operation, status, m.nodeID, stats.Count)
		fmt.Fprintf(&b, "distcache_request_duration_seconds_sum{operation=%q,status=%q,node=%q} %.6f\n", operation, status, m.nodeID, stats.Sum)
	}

	b.WriteString("# HELP distcache_grpc_request_duration_seconds gRPC request duration summary.\n")
	b.WriteString("# TYPE distcache_grpc_request_duration_seconds summary\n")
	for _, key := range sortedDurationKeys(m.grpcDurations) {
		method, status := splitLabelKey(key)
		stats := m.grpcDurations[key]
		fmt.Fprintf(&b, "distcache_grpc_request_duration_seconds_count{operation=%q,status=%q,node=%q} %d\n", method, status, m.nodeID, stats.Count)
		fmt.Fprintf(&b, "distcache_grpc_request_duration_seconds_sum{operation=%q,status=%q,node=%q} %.6f\n", method, status, m.nodeID, stats.Sum)
	}
	return b.String()
}

func labelKey(left, right string) string {
	return left + "\xff" + right
}

func splitLabelKey(key string) (string, string) {
	parts := strings.SplitN(key, "\xff", 2)
	if len(parts) != 2 {
		return key, ""
	}
	return parts[0], parts[1]
}

func sortedKeys(values map[string]int64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedDurationKeys(values map[string]*durationStats) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedHealthKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
