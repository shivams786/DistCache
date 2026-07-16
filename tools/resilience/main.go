package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	totalKeys = 1000
	newWrites = 100
	failedNode = "cache-node-2"
)

func main() {
	client := &http.Client{Timeout: 2 * time.Second}
	base := "http://localhost:8081"
	nodes := []string{"http://localhost:8081", "http://localhost:8082", "http://localhost:8083"}
	availableNodes := []string{"http://localhost:8081", "http://localhost:8083"}

	start := time.Now()
	run("docker", "compose", "up", "-d", "--build")
	initialHealthy := waitForHealthy(client, base, 3, 60*time.Second)

	inserted := 0
	for i := 0; i < totalKeys; i++ {
		value := fmt.Sprintf("value-%d", i)
		if put(client, nodes[i%len(nodes)], key(i), value) == nil {
			inserted++
		}
	}
	replicated := waitForReplication(client, nodes, inserted, 45*time.Second)
	before := readable(client, nodes, 0, totalKeys)

	run("docker", "compose", "stop", failedNode)
	failureStart := time.Now()
	failureDetected := waitForUnhealthy(client, base, 3, 8*time.Second)
	failureDetectionSeconds := time.Since(failureStart).Seconds()

	during := readable(client, availableNodes, 0, totalKeys)
	writesDuringFailure := 0
	for i := totalKeys; i < totalKeys+newWrites; i++ {
		if put(client, availableNodes[i%len(availableNodes)], key(i), fmt.Sprintf("value-%d", i)) == nil {
			writesDuringFailure++
		}
	}

	run("docker", "compose", "start", failedNode)
	recoveryStart := time.Now()
	recovered := waitForHealthy(client, base, 3, 10*time.Second)
	recoveryDetectionSeconds := time.Since(recoveryStart).Seconds()
	time.Sleep(3 * time.Second)

	finalHealthy := healthyCount(client, base)
	finalReadable := readable(client, nodes, 0, totalKeys+newWrites)
	finalAvailability := float64(finalReadable) / float64(totalKeys+newWrites) * 100

	fmt.Printf("Cluster start detected: %v\n", initialHealthy)
	fmt.Printf("Keys inserted: %d\n", inserted)
	fmt.Printf("Replication successes observed: %d\n", replicated)
	fmt.Printf("Keys readable before failure: %d/%d\n", before, totalKeys)
	fmt.Printf("Node stopped: %s\n", failedNode)
	fmt.Printf("Failure detected within 8 seconds: %v\n", failureDetected)
	fmt.Printf("Failure detection time seconds: %.2f\n", failureDetectionSeconds)
	fmt.Printf("Keys readable during failure: %d/%d\n", during, totalKeys)
	fmt.Printf("Successful writes during failure: %d/%d\n", writesDuringFailure, newWrites)
	fmt.Printf("Node recovery detected within 10 seconds: %v\n", recovered)
	fmt.Printf("Recovery detection time seconds: %.2f\n", recoveryDetectionSeconds)
	fmt.Printf("Final healthy nodes: %d/3\n", finalHealthy)
	fmt.Printf("Final readable keys: %d/%d\n", finalReadable, totalKeys+newWrites)
	fmt.Printf("Final read availability percent: %.2f\n", finalAvailability)
	fmt.Printf("Total resilience test duration seconds: %.2f\n", time.Since(start).Seconds())
}

func run(name string, args ...string) {
	cmd := exec.Command(name, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf("Command failed: %s %s\n%s\nerror: %v\n", name, strings.Join(args, " "), output, err)
	}
}

func waitForHealthy(client *http.Client, base string, target int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if healthyCount(client, base) >= target {
			return true
		}
		time.Sleep(500 * time.Millisecond)
	}
	return false
}

func waitForUnhealthy(client *http.Client, base string, allNodes int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if healthyCount(client, base) < allNodes {
			return true
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}

func waitForReplication(client *http.Client, nodes []string, target int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	best := 0
	for time.Now().Before(deadline) {
		best = replicationSuccesses(client, nodes)
		if best >= target {
			return best
		}
		time.Sleep(500 * time.Millisecond)
	}
	return best
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

func readable(client *http.Client, nodes []string, start int, count int) int {
	found := 0
	for i := start; i < start+count; i++ {
		for _, node := range nodes {
			resp, err := client.Get(node + "/cache/" + key(i))
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

func replicationSuccesses(client *http.Client, nodes []string) int {
	total := 0
	for _, node := range nodes {
		resp, err := client.Get(node + "/metrics")
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.HasPrefix(line, "distcache_replication_success_total") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			value, err := strconv.Atoi(fields[1])
			if err == nil {
				total += value
			}
		}
	}
	return total
}

func key(i int) string {
	return fmt.Sprintf("resilience:%d", i)
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
