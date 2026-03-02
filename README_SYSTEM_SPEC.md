# Broker Proxy Specification (ActiveMQ Snapshots → Redis #1 State + Change Index)

## Objective
Design and specify a highly-available **Broker Proxy** that consumes **full snapshot** messages from ActiveMQ for multiple topics, filters duplicates/out-of-order snapshots, computes **UPSERT/DELETE/NO-OP** deltas per producer, persists **Current State** plus a **per-topic SEQ change index** in **Redis #1 (Sentinel)**, enabling downstream **pull-based** synchronization via KOSERVER.

## Assumptions (if any)
1. **Snapshot contract:** For each `(topic, producerId)`, `msgTs` is **strictly increasing** and **unique** (including across restarts).
   - *Impact:* Recency gate correctness depends on this; violations must be detected and alerted.
2. **Version contract:** For each `itemId`, **any semantic change** to the item implies `version` increases (monotonic strict).
   - *Impact:* NO-OP detection is based on version equality; no JSON comparisons are performed.
3. **Redis #1 topology:** **Single primary** with **replicas + Sentinel**, **no Redis Cluster**.
   - *Impact:* One write bottleneck; design must minimize commands, argument size, and CPU spikes.
4. **Read policy:** KOSERVER reads from the **Redis primary only** (not replicas).
   - *Impact:* Avoids replication lag breaking SEQ continuity.
5. **Headsets ownership:** Headsets may transfer between producers, and the chosen policy is **delete+recreate** (no owner-aware delete blocking).
   - *Impact:* An item can temporarily disappear between snapshots; downstream sees `DELETE → UPSERT` during transfers.

## Requirements (bullets)
- **Ingestion**
  - Consume ActiveMQ snapshots for topics: **Computers**, **Headsets**, **Conferences**.
  - Snapshot payload: `topic`, `producerId`, `msgTs`, `items[]` where each item includes `itemId`, `version`, `dataJson`.
- **Dedup / Ordering**
  - Snapshots may be **duplicate** and **out-of-order**; Broker Proxy must enforce a **recency gate** per `(topic, producerId)` using `msgTs`.
- **Delta computation**
  - Snapshots are **full** for a given `(topic, producerId)`.
  - Broker Proxy computes item-level changes between the **last accepted snapshot** and the **new snapshot** for the same `(topic, producerId)`:
    - **UPSERT**: new item or higher version than stored state
    - **DELETE**: item was present in last snapshot, missing in current snapshot
    - **NO-OP**: version unchanged (per contract)
- **Persistence in Redis #1**
  - Current State must be queryable per topic by `itemId`:
    - `itemId -> version`
    - `itemId -> dataJson`
  - Headsets transfer is handled as **DELETE by old producer** then **UPSERT by new producer**.
- **Change index**
  - Maintain **monotonic SEQ per topic** in Redis #1.
  - Maintain two per-topic indices:
    - `upsertedItemIdsBySeq` (“touched/upserted since SEQ”)
    - `deletedItemIdsBySeq`
  - Support **retention** (trim indices) and provide **Full Resync** indication if `sinceSeq` is older than retained history.
- **Pull model**
  - Downstream pulls via KOSERVER: `(topic, sinceSeq, limit)` → `{items[], deleted[], newSeq}`.
  - Pagination must be supported via `limit` and advancing `sinceSeq=newSeq` (no offset pagination).
- **HA**
  - Broker Proxy runs as **3 replicas** with **ZooKeeper leader election**.
  - Single-writer to Redis #1 is enforced using **epoch/fencing** so a stale leader cannot write.
- **Atomicity**
  - Updates to Redis #1 must be logically atomic: recency markers, current state, producer snapshot cache, and change indices must not be left in a partially-applied state visible to readers.
- **Monitoring**
  - Minimal metrics: snapshots received/dropped, upserts/deletes/noops, Redis commit latency, leadership transitions, fencing denials.

## Architecture Overview (ASCII diagram + explanation)
```text
+-------------------+             +----------------------+
| ActiveMQ          |             | ZooKeeper            |
| Topics: C/H/Conf  |             | leader election+epoch |
+---------+---------+             +----------+-----------+
          |                                   |
          | Full Snapshot                     | epoch
          v                                   v
+--------------------------------------------------------+
| Broker Proxy (3 replicas)                              |
|  - only LEADER commits                                |
|  - recency gate per (topic,producerId)                 |
|  - diff engine per producer                            |
|  - atomic commit to Redis #1 (Lua) with epoch fencing  |
+------------------------------+-------------------------+
                               |
                               v
                     +-----------------------+
                     | Redis #1 (Sentinel)   |
                     |  - Current State      |
                     |  - Change Index (SEQ) |
                     |  - Producer cache     |
                     |  - Recency markers    |
                     +----------+------------+
                                |
                                v
                         +--------------+
                         |  KOSERVER     |
                         |  Pull changes |
                         +------+--------+
                                |
                                v
                             KOWRITER
```

Explanation:
- Producers publish **full snapshots** to ActiveMQ.
- Broker Proxy (leader) applies ordering/dedup constraints and computes deltas.
- Broker Proxy persists state and change indices in Redis #1.
- KOSERVER exposes a pull API that reads from Redis #1 primary and returns the **current state** for itemIds that changed since a given SEQ.

## Components & Interfaces (table: component | responsibility | APIs/contracts | data | scaling)
| component | responsibility | APIs/contracts | data | scaling |
|---|---|---|---|---|
| ActiveMQ | delivers snapshot messages | snapshot schema (topic, producerId, msgTs, items[]) | full snapshot payloads | broker HA as deployed |
| ZooKeeper | leader election + monotonic epoch source | ephemeral znode + version/epoch | `epoch`, `leaderId` | 3–5 nodes |
| Broker Proxy | ingest + recency + diff + commit | JMS consumer; internal leader callbacks | delta plan; redis commit | 3 replicas; leader-only writer |
| Redis #1 (Sentinel) | authoritative state + indices | Redis + Lua scripts | hashes + zsets + markers | 1 primary + replicas |
| KOSERVER | pull API for downstream | `GET /changes?topic&sinceSeq&limit` | ids+state+deleted+newSeq | stateless, scale-out |
| KOWRITER | downstream consumer | contract with KOSERVER | consumes deltas | scale-out |

## Redis Design (keys, structures, commands, complexity, atomicity, TTL/retention, sizing)

### Key naming (meaningful + recommended short form)
Topics: `{topic} ∈ computers|headsets|conferences`

**Fencing / Leader**
- `brokerProxy:leaderEpoch` (String/Int) — **bp:leader:epoch**
- `brokerProxy:leaderInstanceId` (String) — **bp:leader:id**

**Recency gate**
- `brokerProxy:recency:lastAcceptedSnapshotTs:{topic}:{producerId}` — **bp:recency:{topic}:{producerId}**

**Producer “last snapshot” cache (for diff + early filter)**
- Computers (single item per snapshot):
  - `brokerProxy:producer:lastSnapshotSingleItemId:computers:{producerId}` — **bp:comp:lastId:{producerId}**
  - `brokerProxy:producer:lastSnapshotSingleItemVersion:computers:{producerId}` — **bp:comp:lastVer:{producerId}**
- Headsets/Conferences:
  - `brokerProxy:producer:lastSnapshotItemVersion:{topic}:{producerId}` (Hash `itemId -> version`) — **bp:prodver:{topic}:{producerId}**

**Current State (Hash per topic — chosen design)**
- `brokerProxy:currentState:version:{topic}` (Hash `itemId -> version`) — **bp:state:v:{topic}**
- `brokerProxy:currentState:dataJson:{topic}` (Hash `itemId -> dataJson`) — **bp:state:j:{topic}**

**SEQ + change indices (touched set, not an event log)**
- `brokerProxy:changeSeq:current:{topic}` (String/Int) — **bp:seq:{topic}**
- `brokerProxy:changeSeq:minRetained:{topic}` (String/Int) — **bp:seq:min:{topic}**
- `brokerProxy:changeIndex:upsertedItemIdsBySeq:{topic}` (ZSET score=seq, member=itemId) — **bp:ch:up:{topic}**
- `brokerProxy:changeIndex:deletedItemIdsBySeq:{topic}` (ZSET score=seq, member=itemId) — **bp:ch:del:{topic}**

### Command-level access patterns (and complexity)

**Broker Proxy (read path for diff)**
- Recency: `GET bp:recency:*` — O(1)
- Producer baseline:
  - Computers: `GET bp:comp:lastId:*`, `GET bp:comp:lastVer:*` — O(1)
  - Headsets/Conferences: `HGETALL bp:prodver:*` — O(N), N≤80/500

**Broker Proxy (write path)**
- All writes are executed by a single **Lua commit** (see atomicity strategy below).
- State updates:
  - `HSET bp:state:v:{topic} itemId version ...` — O(#fields)
  - `HSET bp:state:j:{topic} itemId dataJson ...` — O(#fields)
  - `HDEL` for deletes — O(#fields)
- Producer cache updates:
  - `HSET/HDEL bp:prodver:{topic}:{producerId}` — O(#changed fields)
- Change index:
  - `INCRBY bp:seq:{topic} nChanges` — O(1)
  - `ZADD bp:ch:up:{topic} seq itemId ...` — O(k log M)
  - `ZADD bp:ch:del:{topic} seq itemId ...` — O(k log M)

**KOSERVER (read path)**
- Retention gate: `GET bp:seq:min:{topic}`, `GET bp:seq:{topic}` — O(1)
- Changed ids: `ZRANGEBYSCORE ... WITHSCORES LIMIT 0 limit` — O(log M + K)
- Current state fetch:
  - `HMGET bp:state:v:{topic} id1 ... idK`
  - `HMGET bp:state:j:{topic} id1 ... idK`
  - Complexity O(K)

> Guardrail: never use `HGETALL` on `bp:state:*` in production.

### Atomicity strategy (implemented “red team” fixes)
**Goal:** state + recency + producer cache + change indices must not be visible in a partially-applied state.

**Approach:** single **Lua script** per accepted snapshot for `(topic, producerId)` that:
1. Validates **epoch** (`bp:leader:epoch`) and fails with `FENCED` if mismatch.
2. Validates **recency** for `(topic, producerId)` (`bp:recency:*`); drops if `msgTs <= lastAccepted`.
3. Applies prepared **write plan** (computed in application):
   - `producerCacheFieldUpdates` (only fields whose version changed or new)
   - `producerCacheDeletes`
   - `stateUpserts` (itemId→version and itemId→dataJson)
   - `stateDeletes` (itemIds)
4. Allocates SEQ efficiently with **one counter hit**:
   - `nChanges = upsertCount + deleteCount`
   - `lastSeq = INCRBY bp:seq:{topic} nChanges`
   - `firstSeq = lastSeq - nChanges + 1`
   - Assign unique seq values to each changed item deterministically (order of provided arrays).
5. Writes `ZADD` entries for upserts/deletes with those unique seq values.
6. Updates recency marker to `msgTs`.

**Why INCRBY (not per-item INCR):**
- Avoids a hot counter bottleneck while keeping **unique** seq per item (simple pagination; no “same-score” ties).

### Retention / tombstones (indices)
- Maintain a per-topic retained window by SEQ:
  - leader job periodically:
    - `currentSeq = GET bp:seq:{topic}`
    - `minKeep = currentSeq - RETAIN_WINDOW`
    - `ZREMRANGEBYSCORE bp:ch:up:{topic} -inf minKeep`
    - `ZREMRANGEBYSCORE bp:ch:del:{topic} -inf minKeep`
    - `SET bp:seq:min:{topic} minKeep`
- KOSERVER returns `fullResyncRequired=true` when `sinceSeq < minRetained`.

### Persistence strategy (Redis #1)
Recommended:
- **AOF + RDB**
  - AOF: `appendfsync everysec`
  - RDB snapshots for faster restarts
Operational safety (Sentinel):
- Set `min-replicas-to-write` and `min-replicas-max-lag` to avoid acknowledging writes when replicas are too far behind.

### Memory / sizing method (practical)
- Dominant cost: `dataJson` stored in `bp:state:j:{topic}`.
- Estimate:
  - `totalDataBytes ≈ Σ_topic (numItems_topic * avgDataJsonBytes_topic) * overheadFactor`
  - use overheadFactor 1.5–2.0 for Redis overhead + fragmentation.
- Indices:
  - `bp:ch:*` size depends on `retainWindow` and unique touched ids.

## Data & Consistency Model (ordering, idempotency, dedup, versioning, recovery)

### Ordering & dedup
- For each `(topic, producerId)`:
  - accept iff `msgTs > lastAcceptedMsgTs`
  - otherwise drop snapshot (duplicate/out-of-order) and emit metrics.

### Idempotency
- Duplicate snapshots (same `msgTs`) are dropped by recency gate.
- Resends with unchanged versions produce a **NO-OP** write plan (no SEQ allocation, no index updates), while still updating recency marker (optional; recommended to freeze the last accepted ts).

### Version semantics
- For each `itemId` in current state:
  - if `newVersion > storedVersion` → UPSERT
  - if `newVersion == storedVersion` → NO-OP
  - if `newVersion < storedVersion` → anomaly: drop item update and alert

### Topic-specific semantics
- **Computers**
  - Each snapshot contains exactly one `itemId`.
  - Diff is optimized using single-item baseline keys (no large hash reads).
- **Headsets**
  - Ownership transfer is modeled as `DELETE` (old producer snapshot removed item) then `UPSERT` (new producer snapshot includes item), even if version did not change.
- **Conferences**
  - Up to 500 items per snapshot; early filter via `bp:prodver` is required to avoid reading/updating state for unchanged items.

### Recovery
- Redis restart restores state + indices from AOF/RDB.
- Broker Proxy leader change:
  - stale leader is fenced by epoch mismatch; new leader continues from Redis state.
- Sentinel failover:
  - clients must reconnect to new primary; broker proxy commit must retry.
  - correct configuration of `min-replicas-to-write` reduces lost writes.

## Alternatives (table: decision | option | pros | cons | when)
| decision | option | pros | cons | when |
|---|---|---|---|---|
| Current State model | Hash per topic (chosen) | HMGET efficient; fewer keys | hot key; no per-field TTL | Sentinel topology; pull-by-ids |
| Current State model | Key per itemId | less hot key; per-item TTL | many keys; more RTTs | if write throughput becomes bottleneck |
| SEQ allocation | INCRBY + per-item unique seq (chosen) | 1 counter op; simple pagination | still writes many ZADD pairs if many changes | general case |
| SEQ allocation | single seq per snapshot | fewer scores; simpler index write | pagination needs tie handling; semantics blur | if you accept more KOSERVER complexity |
| Change log | ZSET touched (chosen) | perfect for state-based pull | not a true event log | no replay requirements |
| Change log | Redis Streams | real event log | higher complexity/cost | audit/replay needed |

## Risks & Mitigations

### R1: Hot keys for current state hashes
- **Risk:** `bp:state:j:{conferences}` becomes a hot key under bursts.
- **Mitigation:**
  - enforce strict `limit` on pull responses
  - prohibit `HGETALL` on state keys (ACL/runbooks/alerts)
  - if needed, shard current state into `N` hashes per topic using `shard(itemId)%N` (Plan B).

### R2: Large payloads in commits
- **Risk:** very large `dataJson` increases Redis CPU and network latency.
- **Mitigation:** compress at producer (if allowed) or enforce payload size limits; monitor p99 commit latency and slowlog.

### R3: Sentinel failover and data loss
- **Risk:** async replication can lose writes during failover.
- **Mitigation:** configure `min-replicas-to-write` and `min-replicas-max-lag`; alert on replication lag.

### R4: delete→recreate “holes” for Headsets
- **Risk:** headset may be temporarily missing between snapshots.
- **Mitigation:** explicitly document eventual behavior; monitor rate of delete→upsert within short windows; consider optional grace window if product requirements change.

### R5: Incorrect pagination (skipping changes)
- **Risk:** returning `newSeq=curSeq` can skip changes when `limit` truncates.
- **Mitigation:** KOSERVER must set `newSeq` to **max score returned** (WITHSCORES). Never use offset pagination.

## Observability Plan (metrics/logs/alerts) (include Redis alerts + thresholds)

### Broker Proxy metrics (Prometheus)
- `brokerproxy_snapshots_received_total{topic}`
- `brokerproxy_snapshots_dropped_total{topic,reason=duplicate|out_of_order|fenced}`
- `brokerproxy_items_upsert_total{topic}`
- `brokerproxy_items_delete_total{topic}`
- `brokerproxy_snapshots_noop_total{topic}`
- `brokerproxy_version_anomaly_total{topic}`
- `brokerproxy_redis_commit_duration_seconds_bucket{topic}`
- `brokerproxy_leadership_changes_total`
- `brokerproxy_fencing_denied_total`

Suggested alerts (tune to your SLO):
- Commit latency: `p99(brokerproxy_redis_commit_duration_seconds) > 0.05s for 5m`
- Drops spike: `increase(brokerproxy_snapshots_dropped_total[5m]) / increase(brokerproxy_snapshots_received_total[5m]) > 0.01`
- Version anomalies: `increase(brokerproxy_version_anomaly_total[5m]) > 0`
- Leadership flapping: `increase(brokerproxy_leadership_changes_total[10m]) > 3`

### KOSERVER metrics
- `koserver_pull_requests_total{topic}`
- `koserver_pull_items_returned_total{topic}`
- `koserver_pull_deleted_returned_total{topic}`
- `koserver_pull_resync_required_total{topic}`
- `koserver_redis_read_duration_seconds_bucket{topic}`

Alerts:
- Resync spike: `increase(koserver_pull_resync_required_total[10m]) > X`
- Read latency: `p99(koserver_redis_read_duration_seconds) > 0.03s for 5m`

### Redis exporter metrics & alerts
- Memory: `redis_used_memory_bytes / redis_maxmemory_bytes > 0.85 for 10m`
- Evictions: `increase(redis_evicted_keys_total[5m]) > 0` (**must be 0**)
- Replication: link down > 30s; lag above threshold
- AOF: last write status not ok
- Slowlog: spikes in `SLOWLOG` entries; top offenders include `HGETALL` on state keys

Logs (structured)
- Accept snapshot: `{topic, producerId, msgTs, itemCount, upserts, deletes, noopItems, epoch}`
- Drop snapshot: `{topic, producerId, msgTs, lastAcceptedTs, reason}`
- Version anomaly: `{topic, itemId, newVersion, storedVersion, producerId}`
- Fencing: `{instanceId, epochSeen, epochCurrent}`

## Rollout & Backward Compatibility
- Deploy Broker Proxy writing to a new Redis namespace prefix (e.g., `bp2:*`) in **shadow mode**.
- Compare:
  - counts per topic (number of items, versions distribution)
  - sample of itemIds state equivalence
  - change index progression
- Switch KOSERVER to read from `bp2:*` behind a feature flag.
- Roll back by toggling flag back to old namespace.

## Phase 1 Plan (steps + acceptance criteria + load test plan)

### Steps + acceptance criteria
1. **Redis schema + contracts**
   - AC: key patterns finalized; unit tests validate all key writes/reads.
2. **Leader election + epoch fencing**
   - AC: stale leader cannot commit (fencing metric increments; Redis state unchanged).
3. **Computers topic end-to-end**
   - AC: single-item diff works; itemId change yields delete+upsert; duplicates dropped.
4. **Headsets topic end-to-end**
   - AC: delete+recreate behavior verified; KOSERVER filters `deleted - changed`.
5. **Conferences topic optimization**
   - AC: snapshot with 500 items where 1 changed produces only 1 state upsert and minimal producer-cache writes.
6. **Retention + resync signaling**
   - AC: when `sinceSeq < minRetained`, KOSERVER returns `fullResyncRequired=true`.
7. **Monitoring + dashboards**
   - AC: alerts trigger under injected fault scenarios.

### Load test plan (focus on Redis throughput/latency)
- Inputs (simulate realistic rates; numbers below are per snapshot size constraints):
  - Computers: 5000 producers, 1 item/snapshot
  - Headsets: 120 producers, up to 80 items/snapshot
  - Conferences: 120 producers, up to 500 items/snapshot
- Scenarios:
  1. baseline ordered delivery
  2. 10% duplicates
  3. 5% out-of-order (older msgTs)
  4. headsets transfer storms (delete then recreate)
  5. sentinel failover during steady load
- KPIs:
  - Broker Proxy commit p99 within agreed SLO
  - KOSERVER pull p99 within agreed SLO for limit=100
  - 0 incorrect regressions (no old snapshots applied; no version rollback applied)

## Checklist (10 bullets)
- [ ] Leader-only writes enforced via ZooKeeper + epoch fencing in Redis
- [ ] Recency gate drops duplicates/out-of-order per (topic,producerId)
- [ ] Producer baseline cache exists and is used as mandatory early filter for Conferences
- [ ] Current State stored as Hash per topic (version + dataJson)
- [ ] Change index uses ZSET touched + ZSET deleted per topic
- [ ] SEQ allocation uses INCRBY per snapshot with unique per-item seq values
- [ ] KOSERVER newSeq = max score returned; no offset pagination
- [ ] KOSERVER filters deletedIds = deletedIds - changedIds
- [ ] Retention trims indices and sets minRetained watermark
- [ ] Sentinel safety configured (min-replicas-to-write / max-lag) + alerts in place
