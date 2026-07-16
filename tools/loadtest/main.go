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

func main() {
	urls := strings.Split(env("DISTCACHE_URLS", "http://localhost:8081,http://localhost:8082,http://localhost:8083"), ",")
	clients := envInt("LOAD_CLIENTS", 16)
	duration := envDuration("LOAD_DURATION", 15*time.Second)
	client := &http.Client{Timeout: 2 * time.Second}

	var total, errorsCount int64
	var latencies []time.Duration
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
				if err := issue(client, urls[rng.Intn(len(urls))], rng); err != nil {
					atomic.AddInt64(&errorsCount, 1)
				}
				atomic.AddInt64(&total, 1)
				latMu.Lock()
				latencies = append(latencies, time.Since(start))
				latMu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	avg := time.Duration(0)
	for _, latency := range latencies {
		avg += latency
	}
	if len(latencies) > 0 {
		avg /= time.Duration(len(latencies))
	}
	p95 := time.Duration(0)
	if len(latencies) > 0 {
		p95 = latencies[int(float64(len(latencies)-1)*0.95)]
	}
	elapsed := duration.Seconds()
	fmt.Printf("Requests: %d\n", total)
	fmt.Printf("Requests per second: %.2f\n", float64(total)/elapsed)
	fmt.Printf("Average latency: %s\n", avg)
	fmt.Printf("p95 latency: %s\n", p95)
	fmt.Printf("Error rate: %.2f%%\n", float64(errorsCount)/float64(max(total, 1))*100)
}

func issue(client *http.Client, base string, rng *rand.Rand) error {
	key := fmt.Sprintf("load:%d", rng.Intn(500))
	roll := rng.Intn(100)
	switch {
	case roll < 60:
		resp, err := client.Get(base + "/cache/" + key)
		return closeResp(resp, err)
	case roll < 90:
		body, _ := json.Marshal(map[string]any{"value": fmt.Sprintf("value-%d", rng.Int()), "ttl_seconds": 120})
		req, _ := http.NewRequest(http.MethodPut, base+"/cache/"+key, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		return closeResp(resp, err)
	default:
		req, _ := http.NewRequest(http.MethodDelete, base+"/cache/"+key, nil)
		resp, err := client.Do(req)
		return closeResp(resp, err)
	}
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
