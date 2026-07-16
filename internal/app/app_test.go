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
	"testing"
	"time"

	"github.com/codex/distcache/internal/cluster"
	"github.com/codex/distcache/internal/config"
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
			VirtualNodes:        80,
			ReplicationFactor:   2,
			ReplicationWorkers:  2,
			ReplicationQueue:    32,
			HealthCheckInterval: 100 * time.Millisecond,
			RequestTimeout:      200 * time.Millisecond,
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

func waitForClusterHealthy(t *testing.T, nodes []*testNode) {
	t.Helper()
	eventually(t, 3*time.Second, func() bool {
		for _, appNode := range nodes {
			for _, status := range appNode.app.membership.Snapshot() {
				if !status.Healthy {
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
