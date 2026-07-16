# Acceptance Notes

This file is a quick map from the requested checklist to the current repository. It is not a test result. Commands that require Go or Docker still need to be run in an environment where those tools are installed.

## Implemented In Code

- Three cache nodes are defined in `docker-compose.yml`.
- Cache defaults, health thresholds, replication settings, HTTP timeouts, and recovery limits are loaded in `internal/config/config.go`.
- TTL, LRU eviction, snapshots, and thread-safe cache operations live in `internal/cache`.
- Consistent hashing and replica selection live in `internal/hash` and `internal/cluster`.
- HTTP routing, gRPC peer handlers, health checks, failover, recovery sync, metrics endpoints, and admin APIs live in `internal/app`.
- Bounded asynchronous replication lives in `internal/replication`.
- Prometheus text output lives in `internal/metrics`.
- The dashboard is served from `internal/dashboard`.
- The peer API is described in `proto/cache.proto`.

## Tests And Scripts

- Unit tests cover cache behavior, hashing, membership, configuration, and replication retry behavior.
- In-process integration tests in `internal/app/app_test.go` start three local nodes and cover forwarding, replication, TTL, health endpoints, max value handling, metrics, hop limits, and replica reads.
- `tools/resilience` runs a Docker Compose node-failure scenario and prints observed availability numbers.
- `tools/loadtest` runs the local mixed read/write/delete workload and prints observed latency and error-rate numbers.

## Verification Commands

Run these from the repository root when Go and Docker are available:

```bash
go mod tidy
gofmt -w cmd internal tools
make verify
docker compose config
docker compose up --build
make resilience-test
make load-test
```
