# Resume Description

- Built a three-node distributed cache in Go that routes keys with consistent hashing and supports TTL expiration, LRU eviction, and asynchronous replication over gRPC.
- Added health-based failover, bounded replication workers, Prometheus metrics, structured logs, and a small dashboard for inspecting node and cache behavior.
- Wrote unit and in-process integration tests for cache behavior, consistent hashing, replication retries, health transitions, forwarding, and replica reads during a primary-node failure.
