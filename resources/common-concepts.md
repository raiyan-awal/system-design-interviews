# Common Concepts Reference

Recurring building blocks across all 100 system design questions. Read this once — it won't be repeated in every answer.

---

## 1. CAP Theorem

A distributed system can guarantee only **2 of 3**:

| Property | Meaning |
|----------|---------|
| **Consistency** | Every read gets the most recent write |
| **Availability** | Every request gets a (non-error) response |
| **Partition Tolerance** | System continues operating despite network partitions |

Network partitions are unavoidable in distributed systems, so the real choice is **CP vs AP**:

- **CP** (Consistency + Partition Tolerance): HBase, Zookeeper, etcd — prefer correct data over uptime
- **AP** (Availability + Partition Tolerance): Cassandra, DynamoDB, CouchDB — prefer uptime over perfect consistency
- **CA** systems only exist in single-node setups (e.g., a standalone PostgreSQL)

> Interview tip: state your CAP choice explicitly and justify it with the problem's requirements.

---

## 2. Consistency Models (weakest → strongest)

```
Eventual Consistency
    │  Replicas converge given enough time (e.g., DNS, S3)
    ▼
Monotonic Read Consistency
    │  A client never reads older data after reading newer data
    ▼
Read-Your-Writes Consistency
    │  A client always sees its own writes immediately
    ▼
Session Consistency
    │  Read-your-writes + monotonic reads within a session
    ▼
Causal Consistency
    │  Causally related operations are seen in order by all nodes
    ▼
Strong (Linearizable) Consistency
       Every operation appears instantaneous; all nodes agree on order
```

---

## 3. Database Selection Guide

| Need | Choose |
|------|--------|
| Relational data, ACID transactions | PostgreSQL, MySQL |
| Massive write throughput, wide-column | Cassandra, HBase |
| Document storage, flexible schema | MongoDB |
| Key-value, low latency cache | Redis, Memcached |
| Full-text search | Elasticsearch, OpenSearch |
| Graph relationships | Neo4j, Amazon Neptune |
| Time-series data | InfluxDB, TimescaleDB, Prometheus |
| Analytics / OLAP | Snowflake, BigQuery, ClickHouse, Redshift |
| Immutable ledger / audit | Amazon QLDB, custom append-only log |
| Vector / semantic search | Pinecone, Weaviate, pgvector |

---

## 4. Caching Patterns

### Cache-Aside (Lazy Loading)
```
App reads cache → miss → app fetches DB → app writes to cache → returns data
```
- Pros: only caches what's needed, resilient to cache failure
- Cons: first request is slow (cache miss), risk of stale data

### Write-Through
```
App writes → writes to cache AND DB synchronously
```
- Pros: cache always consistent with DB
- Cons: write latency increases, caches unused data

### Write-Behind (Write-Back)
```
App writes → writes to cache → async flush to DB
```
- Pros: low write latency
- Cons: risk of data loss if cache fails before flush

### Read-Through
```
App reads cache → miss → cache fetches DB → cache stores → returns data
```
- Similar to cache-aside but cache handles the miss logic

### Cache Eviction Policies
| Policy | When to use |
|--------|------------|
| **LRU** (Least Recently Used) | General purpose, most common |
| **LFU** (Least Frequently Used) | When popularity matters more than recency |
| **TTL** (Time-To-Live) | Data with natural expiry (sessions, tokens) |
| **FIFO** | Simple queues, streaming buffers |

---

## 5. Sharding Strategies

### Horizontal Sharding (most common)
Distribute rows across multiple DB nodes by a **shard key**.

| Strategy | How | Good for | Watch out for |
|----------|-----|----------|---------------|
| **Range-based** | shard by value range (user_id 0-1M → shard1) | Time-series, ordered data | Hot spots on sequential keys |
| **Hash-based** | `shard = hash(key) % N` | Even distribution | Range queries span all shards |
| **Consistent Hashing** | Keys and nodes on a ring | Dynamic node adds/removes | Slight imbalance without virtual nodes |
| **Directory-based** | Lookup table maps key → shard | Maximum flexibility | Lookup table is a bottleneck |
| **Geo-based** | Shard by geography | Data locality, compliance | Uneven geographic load |

### Hotspot Problem
If one shard gets disproportionate traffic (e.g., a celebrity user):
- Add a random suffix to the shard key (`user_123_0` ... `user_123_9`)
- Use application-level fan-out reads to aggregate

---

## 6. Replication

| Type | How | Trade-off |
|------|-----|-----------|
| **Single-leader** | All writes go to primary, replicated to followers | Simple, read scalability; primary is bottleneck |
| **Multi-leader** | Multiple primaries accept writes | Better write throughput; conflict resolution needed |
| **Leaderless** | Any node accepts writes (quorum-based) | High availability; eventual consistency |

### Quorum (used in leaderless replication)
- `N` = total replicas, `W` = write quorum, `R` = read quorum
- For strong consistency: `W + R > N`
- Common setup: `N=3, W=2, R=2`

---

## 7. Load Balancing Algorithms

| Algorithm | How | Best for |
|-----------|-----|----------|
| **Round Robin** | Requests distributed sequentially | Homogeneous servers |
| **Weighted Round Robin** | Like round robin but weights by capacity | Mixed server specs |
| **Least Connections** | Route to server with fewest active connections | Long-lived connections |
| **IP Hash** | `hash(client_ip) % N` → same server | Session stickiness |
| **Random** | Pick a server at random | Simple, low overhead |
| **Resource-based** | Route based on CPU/memory metrics | CPU-intensive workloads |

---

## 8. Message Queue Patterns

### Point-to-Point (Queue)
- Producer sends message to a queue
- Only one consumer receives each message
- Used for: task distribution, job queues

### Publish-Subscribe (Topic)
- Producer publishes to a topic
- All subscribers receive every message
- Used for: event broadcasting, fan-out notifications

### Key Kafka Concepts
```
Topic → partitioned into N Partitions
Each Partition → ordered, append-only log
Consumer Group → each partition assigned to one consumer in the group
Offset → consumer tracks position in partition
Retention → messages kept for configurable time (default 7 days)
```

### When to use which
| Scenario | Queue (SQS/RabbitMQ) | Log (Kafka) |
|----------|---------------------|-------------|
| Task distribution | ✓ | |
| Event sourcing / replay | | ✓ |
| Multiple consumers of same event | | ✓ |
| Guaranteed once delivery | ✓ | |
| High throughput streaming | | ✓ |

---

## 9. Rate Limiting Algorithms

| Algorithm | How | Pros | Cons |
|-----------|-----|------|------|
| **Token Bucket** | Tokens added at rate R; request consumes 1 token | Allows bursts up to bucket size | Slightly complex |
| **Leaky Bucket** | Requests added to queue; processed at fixed rate | Smooth output rate | Queue can fill up |
| **Fixed Window Counter** | Count requests per time window | Simple | Burst at window boundary |
| **Sliding Window Log** | Log timestamps of requests; count within window | Accurate | High memory use |
| **Sliding Window Counter** | Hybrid of fixed + log | Accurate + memory efficient | Approximate |

> **Token Bucket** is the most widely used in practice (used by AWS, Stripe, etc.)

---

## 10. Back-of-Envelope Estimation

### Common Numbers to Memorize

| Item | Approximate Value |
|------|------------------|
| L1 cache reference | 1 ns |
| L2 cache reference | 4 ns |
| RAM access | 100 ns |
| SSD random read | 100 µs |
| HDD seek | 10 ms |
| Round-trip within datacenter | 500 µs |
| Cross-continent round-trip | 150 ms |
| 1 Gbps network throughput | 125 MB/s |

### Traffic Estimation Template
```
Daily Active Users (DAU) = X million
Average requests/user/day = Y
Total requests/day = X * Y million
QPS (avg) = total / 86,400
QPS (peak) = avg * 2–5x (rule of thumb)
```

### Storage Estimation Template
```
New items/day = QPS * 86,400
Item size = Z bytes
Daily storage = new_items * Z
Annual storage = daily * 365
With replication (3x) = annual * 3
```

### Bandwidth Estimation
```
Read bandwidth = read_QPS * avg_response_size
Write bandwidth = write_QPS * avg_request_size
```

---

## 11. CDN (Content Delivery Network)

- Edge servers cache static/dynamic content close to users
- **Push CDN**: content pushed to edge proactively (good for known, large static assets)
- **Pull CDN**: content fetched from origin on first request, then cached (good for dynamic/unpredictable content)
- TTL controls cache freshness; use cache-busting (content hash in filename) for versioned assets
- Use CDN for: images, video, JS/CSS, API responses that tolerate staleness

---

## 12. SQL vs NoSQL Decision Framework

Ask these questions:

1. **Do you need ACID transactions?** → SQL
2. **Is your schema fixed?** → SQL; frequently changing? → NoSQL
3. **Are you doing complex joins?** → SQL
4. **Do you need massive horizontal write scale?** → NoSQL
5. **Is your data naturally document/graph/wide-column shaped?** → NoSQL
6. **Do you need full-text search?** → Elasticsearch on top of either

> In practice: most production systems use **both** — SQL for transactional core data, NoSQL/cache for scale-out read paths.

---

## 13. Common Architecture Patterns

### CQRS (Command Query Responsibility Segregation)
- Separate read model from write model
- Writes update the write DB; async events update a read-optimized DB
- Useful when read and write access patterns differ significantly

### Event Sourcing
- Store state as a sequence of events, not current state
- Current state = replay of all events
- Enables audit logs, time travel, event replay
- Pairs naturally with CQRS

### Saga Pattern (Distributed Transactions)
- Break a distributed transaction into local transactions
- **Choreography**: each service emits events that trigger the next step
- **Orchestration**: a central saga orchestrator directs each step
- On failure: compensating transactions undo completed steps

### Circuit Breaker
```
CLOSED → (failures exceed threshold) → OPEN → (timeout) → HALF-OPEN → (success) → CLOSED
```
- Prevents cascading failures when a downstream service is degraded
- Libraries: Hystrix, Resilience4j, Polly

---

## 14. Latency Targets (SLA Reference)

| Type | Typical target |
|------|---------------|
| Interactive read (cached) | < 10 ms |
| Interactive read (DB) | < 100 ms |
| Write (non-critical) | < 200 ms |
| Background job | seconds–minutes |
| Batch pipeline | hours |
| P99 user-facing | < 500 ms |

---

## 15. Numbers Every Engineer Should Know

| Metric | Value |
|--------|-------|
| Characters in a tweet | 280 |
| Avg webpage size | ~2 MB |
| Avg image size | ~300 KB |
| Avg video bitrate (1080p) | ~8 Mbps |
| MySQL rows/second (single node) | ~10K writes/s |
| Redis ops/second | ~100K–1M/s |
| Kafka throughput | ~1M messages/s per broker |
| Cassandra writes/second | ~50K–200K/s per node |
