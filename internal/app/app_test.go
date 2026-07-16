package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/codex/distcache/internal/cluster"
	"github.com/codex/distcache/internal/config"
	"github.com/codex/distcache/internal/transport"
)

type testNode struct {
	id      string
	app     *App
	baseURL string
	cancel  context.CancelFunc
	done    chan error
}

func TestDistributedWriteForwardingAndReplication(t *testing.T) {
	nodes := startTestCluster(t, 3)
	key := keyOwnedBy(t, nodes, "cache-node-2")
	writer := nodeByID(t, nodes, "cache-node-1")

	putValue(t, writer.baseURL, key, "forwarded-value", 30)

	var found getResponse
	eventually(t, time.Second, func() bool {
		found = getValue(t, writer.baseURL, key)
		return found.Found && found.Value == "forwarded-value" && found.ServedBy == "cache-node-2"
	})

	replicaID := replicaFor(t, nodes, key, "cache-node-2")
	replica := nodeByID(t, nodes, replicaID)
	eventually(t, 2*time.Second, func() bool {
		got := getValue(t, replica.baseURL, key)
		return got.Found && got.Value == "forwarded-value"
	})
}

func TestDeletePropagatesToReplica(t *testing.T) {
	nodes := startTestCluster(t, 3)
	key := keyOwnedBy(t, nodes, "cache-node-1")
	owner := nodeByID(t, nodes, "cache-node-1")
	replica := nodeByID(t, nodes, replicaFor(t, nodes, key, "cache-node-1"))

	putValue(t, owner.baseURL, key, "delete-me", 30)
	eventually(t, 2*time.Second, func() bool {
		return getValue(t, replica.baseURL, key).Found
	})

	deleteValue(t, owner.baseURL, key)
	eventually(t, 2*time.Second, func() bool {
		return !getValue(t, replica.baseURL, key).Found
	})
}

func TestTTLExpirationAcrossReplicas(t *testing.T) {
	nodes := startTestCluster(t, 3)
	key := keyOwnedBy(t, nodes, "cache-node-1")
	owner := nodeByID(t, nodes, "cache-node-1")
	replica := nodeByID(t, nodes, replicaFor(t, nodes, key, "cache-node-1"))

	putValue(t, owner.baseURL, key, "short-lived", 1)
	eventually(t, 2*time.Second, func() bool {
		return getValue(t, replica.baseURL, key).Found
	})
	eventually(t, 3*time.Second, func() bool {
		return !getValue(t, owner.baseURL, key).Found && !getValue(t, replica.baseURL, key).Found
	})
}

func TestReadFallsBackToReplicaWhenPrimaryUnavailable(t *testing.T) {
	nodes := startTestCluster(t, 3)
	key := keyOwnedBy(t, nodes, "cache-node-2")
	primary := nodeByID(t, nodes, "cache-node-2")
	replica := nodeByID(t, nodes, replicaFor(t, nodes, key, "cache-node-2"))
	client := nodeByID(t, nodes, "cache-node-1")

	putValue(t, primary.baseURL, key, "still-readable", 30)
	eventually(t, 2*time.Second, func() bool {
		return getValue(t, replica.baseURL, key).Found
	})

	stopNode(t, primary)
	eventually(t, 2*time.Second, func() bool {
		status := nodeByID(t, nodes, "cache-node-1").app.membership.Snapshot()
		for _, node := range status {
			if node.Node.ID == "cache-node-2" {
				return !node.Healthy
			}
		}
		return false
	})

	got := getValue(t, client.baseURL, key)
	if !got.Found || got.Value != "still-readable" || got.ServedBy != replica.id {
		t.Fatalf("expected replica read through %s, got %+v", replica.id, got)
	}
}

func TestHealthLiveAndReadyEndpoints(t *testing.T) {
	nodes := startTestCluster(t, 3)
	node := nodeByID(t, nodes, "cache-node-1")

	expectStatus(t, node.baseURL+"/health/live", http.StatusOK)
	expectStatus(t, node.baseURL+"/health/ready", http.StatusOK)
}

func TestRejectsValuesOverOneMB(t *testing.T) {
	nodes := startTestCluster(t, 3)
	node := nodeByID(t, nodes, "cache-node-1")
	body, err := json.Marshal(map[string]any{"value": strings.Repeat("x", 1024*1024+1)})
	if err != nil {
		t.Fatalf("marshal oversized payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, node.baseURL+"/cache/too-large", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put oversized value: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 413, got %d: %s", resp.StatusCode, data)
	}
}

func TestDefaultTTLAppliedWhenOmitted(t *testing.T) {
	nodes := startTestCluster(t, 3)
	owner := nodeByID(t, nodes, "cache-node-1")
	key := keyOwnedBy(t, nodes, owner.id)
	putValueWithoutTTL(t, owner.baseURL, key, "default-ttl")

	entry, ok := owner.app.cache.Get(key)
	if !ok {
		t.Fatal("expected value to be stored")
	}
	ttl := time.Until(entry.ExpiresAt)
	if ttl < 250*time.Second || ttl > 300*time.Second {
		t.Fatalf("expected default ttl near 300s, got %s", ttl)
	}
}

func TestMetricsEndpointIncludesRequiredMetrics(t *testing.T) {
	nodes := startTestCluster(t, 3)
	node := nodeByID(t, nodes, "cache-node-1")
	putValue(t, node.baseURL, "metrics-key", "metrics-value", 30)

	resp, err := http.Get(node.baseURL + "/metrics")
	if err != nil {
		t.Fatalf("get metrics: %v", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	body := string(data)
	required := []string{
		"distcache_requests_total",
		"distcache_cache_hits_total",
		"distcache_cache_misses_total",
		"distcache_cache_entries",
		"distcache_evictions_total",
		"distcache_expired_entries_total",
		"distcache_replication_success_total",
		"distcache_replication_failures_total",
		"distcache_failovers_total",
		"distcache_node_health",
		"distcache_request_duration_seconds",
		"distcache_grpc_request_duration_seconds",
	}
	for _, metric := range required {
		if !strings.Contains(body, metric) {
			t.Fatalf("metrics output missing %s:\n%s", metric, body)
		}
	}
}

func TestRejectsForwardingHopOverLimit(t *testing.T) {
	nodes := startTestCluster(t, 3)
	node := nodeByID(t, nodes, "cache-node-1")
	_, err := node.app.Set(context.Background(), &transport.SetRequest{
		Key:      "too-many-hops",
		Value:    []byte("value"),
		HopCount: 2,
	})
	if err == nil {
		t.Fatal("expected hop limit error")
	}
}

func startTestCluster(t *testing.T, count int) []*testNode {
	t.Helper()

	nodes := make([]cluster.Node, count)
	httpAddrs := make([]string, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("cache-node-%d", i+1)
		httpAddrs[i] = freeAddr(t)
		nodes[i] = cluster.Node{ID: id, GRPCAddress: freeAddr(t)}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	started := make([]*testNode, 0, count)
	for i := 0; i < count; i++ {
		cfg := config.Config{
			NodeID:              nodes[i].ID,
			HTTPAddr:            httpAddrs[i],
			GRPCAddr:            nodes[i].GRPCAddress,
			ClusterNodes:        nodes,
			CacheMaxEntries:     128,
			CleanupInterval:     50 * time.Millisecond,
			DefaultTTL:          300 * time.Second,
			MaxValueBytes:       1024 * 1024,
			VirtualNodes:        80,
			ReplicationFactor:   2,
			ReplicationWorkers:  8,
			ReplicationQueue:    1000,
			ReplicationRetries:  3,
			HealthCheckInterval: 100 * time.Millisecond,
			RequestTimeout:      200 * time.Millisecond,
			HealthCheckTimeout:  75 * time.Millisecond,
			MaxForwardingHops:   1,
		}
		instance, err := New(cfg, logger)
		if err != nil {
			t.Fatalf("new app: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		node := &testNode{
			id:      nodes[i].ID,
			app:     instance,
			baseURL: "http://" + httpAddrs[i],
			cancel:  cancel,
			done:    make(chan error, 1),
		}
		go func() {
			node.done <- instance.Run(ctx)
		}()
		started = append(started, node)
	}

	for _, node := range started {
		waitForHTTP(t, node.baseURL+"/healthz")
	}
	waitForClusterHealthy(t, started)
	t.Cleanup(func() {
		for _, node := range started {
			stopNode(t, node)
		}
	})
	return started
}

func stopNode(t *testing.T, node *testNode) {
	t.Helper()
	if node.cancel == nil {
		return
	}
	node.cancel()
	select {
	case <-node.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("node %s did not stop", node.id)
	}
	node.cancel = nil
}

func keyOwnedBy(t *testing.T, nodes []*testNode, owner string) string {
	t.Helper()
	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("integration:%s:%d", owner, i)
		got, _, _ := nodes[0].app.membership.WriteOwner(key, 2)
		if got == owner {
			return key
		}
	}
	t.Fatalf("could not find key owned by %s", owner)
	return ""
}

func replicaFor(t *testing.T, nodes []*testNode, key, primary string) string {
	t.Helper()
	replica, ok := nodes[0].app.membership.ReplicaFor(key, primary)
	if !ok {
		t.Fatalf("expected replica for %s", key)
	}
	return replica
}

func nodeByID(t *testing.T, nodes []*testNode, id string) *testNode {
	t.Helper()
	for _, node := range nodes {
		if node.id == id {
			return node
		}
	}
	t.Fatalf("node %s not found", id)
	return nil
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

func waitForHTTP(t *testing.T, url string) {
	t.Helper()
	eventually(t, 2*time.Second, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
}

func expectStatus(t *testing.T, url string, status int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != status {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected %d from %s, got %d: %s", status, url, resp.StatusCode, data)
	}
}

func waitForClusterHealthy(t *testing.T, nodes []*testNode) {
	t.Helper()
	eventually(t, 3*time.Second, func() bool {
		for _, appNode := range nodes {
			for _, status := range appNode.app.membership.Snapshot() {
				if !status.Healthy || status.ConsecutiveSuccess == 0 {
					return false
				}
			}
		}
		return true
	})
}

func eventually(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func putValue(t *testing.T, baseURL, key, value string, ttlSeconds int) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"value": value, "ttl_seconds": ttlSeconds})
	if err != nil {
		t.Fatalf("marshal put payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, baseURL+"/cache/"+key, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new put request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("put status %d: %s", resp.StatusCode, data)
	}
}

func putValueWithoutTTL(t *testing.T, baseURL, key, value string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"value": value})
	if err != nil {
		t.Fatalf("marshal put payload: %v", err)
	}
	req, err := http.NewRequest(http.MethodPut, baseURL+"/cache/"+key, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new put request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("put status %d: %s", resp.StatusCode, data)
	}
}

func deleteValue(t *testing.T, baseURL, key string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, baseURL+"/cache/"+key, nil)
	if err != nil {
		t.Fatalf("new delete request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete %s: %v", key, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("delete status %d: %s", resp.StatusCode, data)
	}
}

type getResponse struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Found    bool   `json:"found"`
	ServedBy string `json:"served_by"`
}

func getValue(t *testing.T, baseURL, key string) getResponse {
	t.Helper()
	resp, err := http.Get(baseURL + "/cache/" + key)
	if err != nil {
		t.Fatalf("get %s: %v", key, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read get body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status %d: %s", resp.StatusCode, data)
	}
	var parsed getResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("decode get response %s: %v", data, err)
	}
	return parsed
}
