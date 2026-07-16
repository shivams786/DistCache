package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"time"
)

func main() {
	client := &http.Client{Timeout: 2 * time.Second}
	base := "http://localhost:8081"
	nodes := []string{"http://localhost:8081", "http://localhost:8082", "http://localhost:8083"}

	waitForCluster(client, base)
	inserted := 0
	for i := 0; i < 100; i++ {
		if put(client, nodes[i%len(nodes)], fmt.Sprintf("resilience:%d", i), fmt.Sprintf("value-%d", i)) == nil {
			inserted++
		}
	}
	before := readable(client, nodes, 100)
	_ = exec.Command("docker", "compose", "stop", "cache-node-2").Run()
	time.Sleep(5 * time.Second)
	during := readable(client, []string{"http://localhost:8081", "http://localhost:8083"}, 100)
	writesDuringFailure := 0
	for i := 100; i < 120; i++ {
		if put(client, "http://localhost:8083", fmt.Sprintf("resilience:%d", i), fmt.Sprintf("value-%d", i)) == nil {
			writesDuringFailure++
		}
	}
	_ = exec.Command("docker", "compose", "start", "cache-node-2").Run()
	recovered := waitForHealthy(client, base, 3, 40*time.Second)

	fmt.Printf("Keys inserted: %d\n", inserted)
	fmt.Printf("Keys available before failure: %d\n", before)
	fmt.Printf("Node stopped: cache-node-2\n")
	fmt.Printf("Keys available during failure: %d\n", during)
	fmt.Printf("Successful writes during failure: %d\n", writesDuringFailure)
	fmt.Printf("Node recovery detected: %v\n", recovered)
	fmt.Printf("Final healthy nodes: %d/3\n", healthyCount(client, base))
}

func waitForCluster(client *http.Client, base string) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if healthyCount(client, base) == 3 {
			return
		}
		time.Sleep(time.Second)
	}
}

func waitForHealthy(client *http.Client, base string, target int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if healthyCount(client, base) >= target {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func healthyCount(client *http.Client, base string) int {
	resp, err := client.Get(base + "/cluster/health")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var payload struct {
		Nodes []struct {
			Healthy bool `json:"healthy"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return 0
	}
	count := 0
	for _, node := range payload.Nodes {
		if node.Healthy {
			count++
		}
	}
	return count
}

func put(client *http.Client, base, key, value string) error {
	body, _ := json.Marshal(map[string]any{"value": value, "ttl_seconds": 300})
	req, _ := http.NewRequest(http.MethodPut, base+"/cache/"+key, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	return closeResp(resp, err)
}

func readable(client *http.Client, nodes []string, count int) int {
	found := 0
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("resilience:%d", i)
		for _, node := range nodes {
			resp, err := client.Get(node + "/cache/" + key)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK && bytes.Contains(body, []byte(`"found":true`)) {
				found++
				break
			}
		}
	}
	return found
}

func closeResp(resp *http.Response, err error) error {
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 500 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
