package config

import (
	"testing"
	"time"
)

func TestLoadExactAcceptanceDefaults(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NODE_ID", "cache-node-1")
	t.Setenv("HTTP_PORT", "8080")
	t.Setenv("GRPC_PORT", "9090")
	t.Setenv("CLUSTER_NODES", "cache-node-1:9090,cache-node-2:9090,cache-node-3:9090")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.CacheMaxEntries != 10000 {
		t.Fatalf("cache max entries = %d", cfg.CacheMaxEntries)
	}
	if cfg.DefaultTTL != 300*time.Second {
		t.Fatalf("default ttl = %s", cfg.DefaultTTL)
	}
	if cfg.CleanupInterval != 5*time.Second {
		t.Fatalf("cleanup interval = %s", cfg.CleanupInterval)
	}
	if cfg.MaxValueBytes != 1024*1024 {
		t.Fatalf("max value bytes = %d", cfg.MaxValueBytes)
	}
	if cfg.VirtualNodes != 100 || cfg.ReplicationFactor != 2 {
		t.Fatalf("ring config virtual=%d rf=%d", cfg.VirtualNodes, cfg.ReplicationFactor)
	}
	if cfg.ReplicationWorkers != 8 || cfg.ReplicationQueue != 1000 || cfg.ReplicationRetries != 3 {
		t.Fatalf("replication config workers=%d queue=%d retries=%d", cfg.ReplicationWorkers, cfg.ReplicationQueue, cfg.ReplicationRetries)
	}
	if cfg.HealthCheckInterval != 2*time.Second || cfg.HealthCheckTimeout != 500*time.Millisecond {
		t.Fatalf("health timing interval=%s timeout=%s", cfg.HealthCheckInterval, cfg.HealthCheckTimeout)
	}
	if cfg.HealthFailureThreshold != 3 || cfg.HealthRecoveryThreshold != 2 {
		t.Fatalf("health thresholds failure=%d recovery=%d", cfg.HealthFailureThreshold, cfg.HealthRecoveryThreshold)
	}
	if cfg.RequestTimeout != 750*time.Millisecond {
		t.Fatalf("request timeout = %s", cfg.RequestTimeout)
	}
	if cfg.GracefulShutdownTimeout != 10*time.Second {
		t.Fatalf("graceful shutdown timeout = %s", cfg.GracefulShutdownTimeout)
	}
	if cfg.RecoverySyncBatchSize != 250 || cfg.RecoverySyncTimeout != 30*time.Second || cfg.MaxConcurrentRecovery != 2 {
		t.Fatalf("recovery config batch=%d timeout=%s concurrency=%d", cfg.RecoverySyncBatchSize, cfg.RecoverySyncTimeout, cfg.MaxConcurrentRecovery)
	}
	if cfg.HTTPReadTimeout != 3*time.Second || cfg.HTTPWriteTimeout != 5*time.Second || cfg.HTTPIdleTimeout != 30*time.Second {
		t.Fatalf("http timeouts read=%s write=%s idle=%s", cfg.HTTPReadTimeout, cfg.HTTPWriteTimeout, cfg.HTTPIdleTimeout)
	}
	if cfg.MaxForwardingHops != 1 {
		t.Fatalf("max forwarding hops = %d", cfg.MaxForwardingHops)
	}
}

func TestLoadAllowsOverrides(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("NODE_ID", "cache-node-9")
	t.Setenv("CLUSTER_NODES", "cache-node-9=127.0.0.1:19090")
	t.Setenv("DEFAULT_TTL", "42s")
	t.Setenv("MAX_VALUE_BYTES", "512")
	t.Setenv("REPLICATION_WORKERS", "2")
	t.Setenv("HEALTH_FAILURE_THRESHOLD", "5")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.DefaultTTL != 42*time.Second {
		t.Fatalf("default ttl override = %s", cfg.DefaultTTL)
	}
	if cfg.MaxValueBytes != 512 {
		t.Fatalf("max value override = %d", cfg.MaxValueBytes)
	}
	if cfg.ReplicationWorkers != 2 {
		t.Fatalf("replication workers override = %d", cfg.ReplicationWorkers)
	}
	if cfg.HealthFailureThreshold != 5 {
		t.Fatalf("health threshold override = %d", cfg.HealthFailureThreshold)
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"NODE_ID",
		"HTTP_PORT",
		"GRPC_PORT",
		"HTTP_ADDR",
		"GRPC_ADDR",
		"CLUSTER_NODES",
		"CACHE_MAX_ENTRIES",
		"DEFAULT_TTL",
		"CACHE_CLEANUP_INTERVAL",
		"MAX_VALUE_BYTES",
		"VIRTUAL_NODES",
		"REPLICATION_FACTOR",
		"REPLICATION_WORKERS",
		"REPLICATION_QUEUE",
		"REPLICATION_RETRIES",
		"HEALTH_CHECK_INTERVAL",
		"HEALTH_CHECK_TIMEOUT",
		"HEALTH_FAILURE_THRESHOLD",
		"HEALTH_RECOVERY_THRESHOLD",
		"REQUEST_TIMEOUT",
		"GRACEFUL_SHUTDOWN_TIMEOUT",
		"RECOVERY_SYNC_BATCH_SIZE",
		"RECOVERY_SYNC_TIMEOUT",
		"MAX_CONCURRENT_RECOVERY",
		"HTTP_READ_TIMEOUT",
		"HTTP_WRITE_TIMEOUT",
		"HTTP_IDLE_TIMEOUT",
		"MAX_FORWARDING_HOPS",
	} {
		t.Setenv(key, "")
	}
}
