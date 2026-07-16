package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultClients  = 50
	defaultDuration = 60 * time.Second
	defaultKeyspace = 10000
	minValueSize    = 100
	maxValueSize    = 1024
)

type result struct {
	total      int64
	errors     int64
	gets       int64
	sets       int64
	deletes    int64
	latencies  []time.Duration
	hitRatio   float64
	replFails  int64
	failovers  int64
}

func main() {
	urls := splitURLs(env("DISTCACHE_URLS", "http://localhost:8081,http://localhost:8082,http://localhost:8083"))
	clients := envInt("LOAD_CLIENTS", defaultClients)
	duration := envDuration("LOAD_DURATION", defaultDuration)
	keyspace := envInt("LOAD_KEYSPACE", defaultKeyspace)
	client := &http.Client{Timeout: 2 * time.Second}

	results := runLoad(client, urls, clients, duration, keyspace)
	stats := clusterStats(client, urls)
	results.hitRatio = stats.hitRatio
	results.replFails = stats.replicationFailures
	results.failovers = stats.failovers
	printResults(results, duration)
}

func runLoad(client *http.Client, urls []string, clients int, duration time.Duration, keyspace int) result {
	var total, errorsCount, gets, sets, deletes int64
	latencies := make([]time.Duration, 0, clients*1000)
	var latMu sync.Mutex
	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup

	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			for time.Now().Before(deadline) {
				start := time.Now()
				op, err := issue(client, urls[rng.Intn(len(urls))], rng, keyspace)
				if err != nil {
					atomic.AddInt64(&errorsCount, 1)
				}
				switch op {
				case "get":
					atomic.AddInt64(&gets, 1)
				case "set":
					atomic.AddInt64(&sets, 1)
				case "delete":
					atomic.AddInt64(&deletes, 1)
				}
				atomic.AddInt64(&total, 1)
				latMu.Lock()
				latencies = append(latencies, time.Since(start))
				latMu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return result{
		total:     total,
		errors:    errorsCount,
		gets:      gets,
		sets:      sets,
		deletes:   deletes,
		latencies: latencies,
	}
}

func issue(client *http.Client, base string, rng *rand.Rand, keyspace int) (string, error) {
	key := fmt.Sprintf("load:%d", rng.Intn(keyspace))
	roll := rng.Intn(100)
	switch {
	case roll < 60:
		resp, err := client.Get(base + "/cache/" + key)
		return "get", closeResp(resp, err)
	case roll < 90:
		body, _ := json.Marshal(map[string]any{
			"value":       randomValue(rng),
			"ttl_seconds": 300,
		})
		req, _ := http.NewRequest(http.MethodPut, base+"/cache/"+key, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		return "set", closeResp(resp, err)
	default:
		req, _ := http.NewRequest(http.MethodDelete, base+"/cache/"+key, nil)
		resp, err := client.Do(req)
		return "delete", closeResp(resp, err)
	}
}

func randomValue(rng *rand.Rand) string {
	size := minValueSize + rng.Intn(maxValueSize-minValueSize+1)
	data := make([]byte, size)
	for i := range data {
		data[i] = byte('a' + rng.Intn(26))
	}
	return string(data)
}

type statSnapshot struct {
	hitRatio            float64
	replicationFailures int64
	failovers           int64
}

func clusterStats(client *http.Client, urls []string) statSnapshot {
	var hits, misses, replFails, failovers int64
	for _, base := range urls {
		resp, err := client.Get(base + "/admin/api/stats")
		if err != nil {
			continue
		}
		var payload struct {
			Cache struct {
				Hits   int64 `json:"hits"`
				Misses int64 `json:"misses"`
			} `json:"cache"`
			Metrics struct {
				ReplicationFailures int64 `json:"replication_failures"`
				Failovers           int64 `json:"failovers"`
			} `json:"metrics"`
		}
		err = json.NewDecoder(resp.Body).Decode(&payload)
		_ = resp.Body.Close()
		if err != nil {
			continue
		}
		hits += payload.Cache.Hits
		misses += payload.Cache.Misses
		replFails += payload.Metrics.ReplicationFailures
		failovers += payload.Metrics.Failovers
	}
	ratio := 0.0
	if hits+misses > 0 {
		ratio = float64(hits) / float64(hits+misses) * 100
	}
	return statSnapshot{hitRatio: ratio, replicationFailures: replFails, failovers: failovers}
}

func printResults(results result, duration time.Duration) {
	sort.Slice(results.latencies, func(i, j int) bool { return results.latencies[i] < results.latencies[j] })
	avg := time.Duration(0)
	for _, latency := range results.latencies {
		avg += latency
	}
	if len(results.latencies) > 0 {
		avg /= time.Duration(len(results.latencies))
	}
	rps := float64(results.total) / duration.Seconds()
	errRate := float64(results.errors) / float64(max(results.total, 1)) * 100

	fmt.Printf("Total requests: %d\n", results.total)
	fmt.Printf("Requests per second: %.2f\n", rps)
	fmt.Printf("GET requests: %d\n", results.gets)
	fmt.Printf("SET requests: %d\n", results.sets)
	fmt.Printf("DELETE requests: %d\n", results.deletes)
	fmt.Printf("Average latency: %s\n", avg)
	fmt.Printf("p50 latency: %s\n", percentile(results.latencies, 0.50))
	fmt.Printf("p95 latency: %s\n", percentile(results.latencies, 0.95))
	fmt.Printf("p99 latency: %s\n", percentile(results.latencies, 0.99))
	fmt.Printf("Error rate: %.2f%%\n", errRate)
	fmt.Printf("Cache hit ratio: %.2f%%\n", results.hitRatio)
	fmt.Printf("Replication failures: %d\n", results.replFails)
	fmt.Printf("Failover count: %d\n", results.failovers)
	fmt.Printf("Target rps met: %v\n", rps >= 2000)
	fmt.Printf("Target p95 met: %v\n", percentile(results.latencies, 0.95) < 50*time.Millisecond)
	fmt.Printf("Target error rate met: %v\n", errRate < 1)
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(float64(len(values)-1) * p)
	return values[idx]
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

func splitURLs(raw string) []string {
	parts := strings.Split(raw, ",")
	urls := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			urls = append(urls, part)
		}
	}
	return urls
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	var value int
	if _, err := fmt.Sscanf(os.Getenv(key), "%d", &value); err == nil {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if parsed, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return parsed
	}
	return fallback
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
