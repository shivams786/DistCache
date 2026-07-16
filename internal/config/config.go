package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/codex/distcache/internal/cluster"
)

type Config struct {
	NodeID                    string
	HTTPAddr                  string
	GRPCAddr                  string
	ClusterNodes              []cluster.Node
	CacheMaxEntries           int
	DefaultTTL                time.Duration
	CleanupInterval           time.Duration
	MaxValueBytes             int
	VirtualNodes              int
	ReplicationFactor         int
	ReplicationWorkers        int
	ReplicationQueue          int
	ReplicationRetries        int
	HealthCheckInterval       time.Duration
	HealthCheckTimeout        time.Duration
	HealthFailureThreshold    int
	HealthRecoveryThreshold   int
	RequestTimeout            time.Duration
	GracefulShutdownTimeout   time.Duration
	RecoverySyncBatchSize     int
	RecoverySyncTimeout       time.Duration
	MaxConcurrentRecovery     int
	HTTPReadTimeout           time.Duration
	HTTPWriteTimeout          time.Duration
	HTTPIdleTimeout           time.Duration
	MaxForwardingHops         int
}

func Load() (Config, error) {
	nodeID := envString("NODE_ID", "cache-node-1")
	httpPort := envInt("HTTP_PORT", 8080)
	grpcPort := envInt("GRPC_PORT", 9090)
	httpAddr := envString("HTTP_ADDR", ":"+strconv.Itoa(httpPort))
	grpcAddr := envString("GRPC_ADDR", ":"+strconv.Itoa(grpcPort))
	clusterRaw := envString("CLUSTER_NODES", fmt.Sprintf("%s=127.0.0.1:%d", nodeID, grpcPort))

	nodes, err := cluster.ParseNodes(clusterRaw)
	if err != nil {
		return Config{}, err
	}
	foundSelf := false
	for _, node := range nodes {
		if node.ID == nodeID {
			foundSelf = true
			break
		}
	}
	if !foundSelf {
		nodes = append(nodes, cluster.Node{ID: nodeID, GRPCAddress: "127.0.0.1:" + strconv.Itoa(grpcPort)})
	}

	return Config{
		NodeID:                  nodeID,
		HTTPAddr:                httpAddr,
		GRPCAddr:                grpcAddr,
		ClusterNodes:            nodes,
		CacheMaxEntries:         envInt("CACHE_MAX_ENTRIES", 10000),
		DefaultTTL:              envDuration("DEFAULT_TTL", 300*time.Second),
		CleanupInterval:         envDuration("CACHE_CLEANUP_INTERVAL", 5*time.Second),
		MaxValueBytes:           envInt("MAX_VALUE_BYTES", 1024*1024),
		VirtualNodes:            envInt("VIRTUAL_NODES", 100),
		ReplicationFactor:       envInt("REPLICATION_FACTOR", 2),
		ReplicationWorkers:      envInt("REPLICATION_WORKERS", 8),
		ReplicationQueue:        envInt("REPLICATION_QUEUE", 1000),
		ReplicationRetries:      envInt("REPLICATION_RETRIES", 3),
		HealthCheckInterval:     envDuration("HEALTH_CHECK_INTERVAL", 2*time.Second),
		HealthCheckTimeout:      envDuration("HEALTH_CHECK_TIMEOUT", 500*time.Millisecond),
		HealthFailureThreshold:  envInt("HEALTH_FAILURE_THRESHOLD", 3),
		HealthRecoveryThreshold: envInt("HEALTH_RECOVERY_THRESHOLD", 2),
		RequestTimeout:          envDuration("REQUEST_TIMEOUT", 750*time.Millisecond),
		GracefulShutdownTimeout: envDuration("GRACEFUL_SHUTDOWN_TIMEOUT", 10*time.Second),
		RecoverySyncBatchSize:   envInt("RECOVERY_SYNC_BATCH_SIZE", 250),
		RecoverySyncTimeout:     envDuration("RECOVERY_SYNC_TIMEOUT", 30*time.Second),
		MaxConcurrentRecovery:   envInt("MAX_CONCURRENT_RECOVERY", 2),
		HTTPReadTimeout:         envDuration("HTTP_READ_TIMEOUT", 3*time.Second),
		HTTPWriteTimeout:        envDuration("HTTP_WRITE_TIMEOUT", 5*time.Second),
		HTTPIdleTimeout:         envDuration("HTTP_IDLE_TIMEOUT", 30*time.Second),
		MaxForwardingHops:       envInt("MAX_FORWARDING_HOPS", 1),
	}, nil
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
