# DistCache Benchmark Results

Date:
Machine:
Go version:
Docker version:
Command:

```bash
make load-test
```

## Configuration

- Nodes: 3
- Replication factor: 2
- Virtual nodes per physical node: 100
- Maximum entries per node: 10,000
- Default TTL: 300 seconds
- Cleanup interval: 5 seconds
- Maximum value size: 1 MB
- Virtual users: 50
- Duration: 60 seconds
- Workload: 60% GET, 30% SET, 10% DELETE
- Key space: 10,000 keys
- Value size: 100 bytes to 1 KB

## Observed Results

| Metric | Observed | Preferred local target |
| --- | ---: | ---: |
| Total requests |  |  |
| Requests per second |  | >= 2,000 |
| Average latency |  |  |
| p50 latency |  |  |
| p95 latency |  | < 50 ms |
| p99 latency |  |  |
| Error rate |  | < 1% |
| Cache hit ratio |  |  |
| Replication failures |  | 0 preferred |
| Failover count |  |  |

## Resilience Results

Command:

```bash
make resilience-test
```

| Metric | Observed | Target |
| --- | ---: | ---: |
| Keys inserted |  | 1,000 |
| Replication successes observed |  | >= 1,000 |
| Keys readable before failure |  | 1,000 / 1,000 |
| Failure detected within 8 seconds |  | true |
| Keys readable during failure |  | >= 990 / 1,000 |
| Successful writes during failure |  | >= 99 / 100 |
| Recovery detected within 10 seconds |  | true |
| Final healthy nodes |  | 3 / 3 |
| Final read availability |  | >= 99% |

## Notes

Record actual numbers honestly. If the local machine does not meet the preferred performance targets, keep the observed results and note CPU, memory, Docker, or network constraints.
