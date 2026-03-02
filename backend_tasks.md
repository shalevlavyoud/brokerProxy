# Backend Tasks — Broker Proxy + KOSERVER (Sequential w/ Approval Gates)

## System Context (Relevant for Execution)

**Purpose**
- Build a Broker Proxy that consumes **full snapshots** from ActiveMQ topics (`conferences`, `computers`, `headsets`), computes per-item deltas, and commits:
  1) current state (versions + dataJson) and  
  2) per-topic change indices based on a monotonic **SEQ**  
  into **Redis #1** (Sentinel).  
- A separate service, **KOSERVER**, exposes a pull API (`/changes`) so downstream components can fetch “what changed since SEQ X”.

**Core invariants**
- Snapshots are the only input. Each snapshot includes: `topic`, `producerId`, `msgTs`, and a full list of items for that producer in that topic.
- `msgTs` MUST be strictly increasing per `(topic, producerId)`. If not, the snapshot is dropped (recency gate).
- Each `item` has `{itemId, version, dataJson}`. `version` is expected to be monotonically increasing per item. If `newVersion < storedVersion`, it is an anomaly (policy-driven handling).
- **Atomicity requirement:** no partial state must be visible—commit is done as a single Lua script.
- **Fencing requirement:** only the current leader instance may commit. Stale leaders must be blocked using `epoch` fencing.
- **SEQ semantics:** SEQ is monotonic per topic and is used for change tracking. When returning paginated results, `newSeq` must be the **max SEQ returned**, not the current topic SEQ (to avoid gaps when `limit` truncates).

**Data model (Redis prefixes are examples; keep consistent across services)**
- Recency: `bp:recency:{topic}:{producerId}` → last accepted `msgTs`
- Leader fencing: `bp:leader:epoch` → current epoch/term (from leader election)
- Per-topic monotonic seq: `bp:seq:{topic}` → INCRBY allocator
- Minimum retained seq watermark: `bp:seq:min:{topic}` → retention watermark
- Current state:
  - Versions: `bp:state:v:{topic}` (HASH `itemId -> version`)
  - Payloads: `bp:state:j:{topic}` (HASH `itemId -> dataJson`)
- Change indices (per topic):
  - Upserts touched-index: `bp:ch:up:{topic}` (ZSET `itemId -> seq`)
  - Deletes touched-index: `bp:ch:del:{topic}` (ZSET `itemId -> seq`)
- Producer cache for efficient diff: `bp:prodver:{topic}:{producerId}` (HASH `itemId -> version`)

**KOSERVER API contract (v1)**
- `GET /changes?topic={t}&sinceSeq={n}&limit={k}`
- Response: `{ items: [...], deleted: [...], newSeq, fullResyncRequired }`
  - `fullResyncRequired=true` when `sinceSeq < bp:seq:min:{topic}`
  - `newSeq = max(seq returned)`; if nothing returned, `newSeq = sinceSeq`

**Operational constraints**
- Redis is deployed with **Sentinel** (single primary + replicas). Failover can happen; clients must reconnect.
- The environment is on-prem; aim for deterministic behavior and strong observability (metrics + logs).


---

## Working Rule (Non-negotiable)
After each task the agent MUST stop and wait for your explicit approval:
- Approve: `APPROVED: BE-XX`
- Request changes: `CHANGES: ...`

The agent may not start the next task until approval is received.

---

## BE-01 — Project Skeleton + Health
**Goal:** Stand up Vert.x services skeleton + config model + structured logging + `/health`.  
**Steps**
1. Create Gradle/Maven project (Java 21, Vert.x).
2. Add config loader (env + file) for: redis prefix, topics list, limits.
3. Implement structured logging baseline (requestId, topic, producerId).
4. Implement `/health` endpoint returning 200 + build/version info.

**Definition of Done (DoD)**
- Service starts locally with `./run` (or `gradle run`).
- `GET /health` returns `200` and JSON `{status:"ok", version:"...", build:"..."}`.

**Verification**
- Local run + curl `/health`.
- CI unit test: health endpoint responds.

**STOP — Approval required**
> Reply: `APPROVED: BE-01` or `CHANGES: ...`

---

## BE-02 — JMS Consumer per Topic + Parsing + Backpressure
**Goal:** Consume snapshots from ActiveMQ per topic and parse into internal model with safe backpressure.  
**Steps**
1. Implement JMS connection + session factory.
2. Create one consumer per topic (config-driven).
3. Parse incoming message into `Snapshot(topic, producerId, msgTs, items[])`.
4. Add bounded queue or Vert.x backpressure strategy (drop/slowdown configurable).

**DoD**
- Snapshots consumed and parsed into internal objects (logged).
- Backpressure prevents OOM under burst (bounded queue + metrics).

**Verification**
- Run against a local ActiveMQ in docker-compose and publish sample snapshot.
- Stress send N snapshots quickly; process remains stable.

**STOP — Approval required**
> Reply: `APPROVED: BE-02` or `CHANGES: ...`

---

## BE-03 — Recency Gate (msgTs) + Drop Reasons + Metrics
**Goal:** Enforce recency per `(topic, producerId)` using Redis and drop duplicates/out-of-order.  
**Steps**
1. Read last accepted from `bp:recency:{topic}:{producerId}`.
2. If `msgTs <= lastAccepted` → drop snapshot, increment `bp_dropped_total{reason="RECENCY"}`.
3. Ensure recency is updated only on successful commit (in Lua) to avoid false-accept markers.
4. Log structured event on drop/accept with fields: topic, producerId, msgTs, lastAccepted.

**DoD**
- Duplicate/out-of-order snapshots do not mutate state.
- Drop reasons include: `RECENCY`, `FENCED`, `VERSION_ANOMALY`, `REDIS_ERROR`.
- Metrics endpoint exposes drop counters.

**Verification**
- Integration test: send snapshots with msgTs 10,9,10 => only first applied.
- Metrics reflect 2 drops.

**STOP — Approval required**
> Reply: `APPROVED: BE-03` or `CHANGES: ...`

---

## BE-04 — Computers Diff (Optimized Baseline)
**Goal:** Implement diff for `computers` using fast baseline keys (single-item snapshots).  
**Steps**
1. Implement baseline read (either from a dedicated key per producer or from producer-cache if you unify logic).
2. If new itemId differs → plan delete old + upsert new.
3. If same itemId and version unchanged → NOOP.
4. If version increased → upsert.
5. Emit `WritePlan(upserts[], deletes[], cacheUpdates[])`.

**DoD**
- Correct UPSERT/DELETE/NOOP computed for computers.
- `WritePlan` size and reasons are logged.

**Verification**
- Unit tests (golden cases): same id same ver, same id higher ver, switched id.
- Negative test: newVersion < oldVersion triggers anomaly handling.

**STOP — Approval required**
> Reply: `APPROVED: BE-04` or `CHANGES: ...`

---

## BE-05 — Headsets/Conferences Diff (Producer Cache Hash)
**Goal:** Implement diff for headsets + conferences using `bp:prodver:{topic}:{producerId}` to early-filter unchanged items.  
**Steps**
1. Read producer cache hash for `(topic, producerId)` to get stored versions for that producer.
2. For each incoming item:
   - If newVersion > stored → UPSERT
   - If == stored → NOOP
   - If < stored → anomaly (metric + policy)
3. Compute deletions: keys in cache but missing in snapshot → DELETE.
4. Update cache plan: set versions for upserts, remove deleted keys.
5. Headsets ownership-transfer uses current policy: modeled as DELETE+UPSERT (no special grace window unless configured).

**DoD**
- Conferences: 500 items with 1 change yields plan upserts=1, deletes=0.
- Deletions computed correctly and reflected in write plan.
- Version anomalies counted in metrics.

**Verification**
- Unit tests: large unchanged snapshot, one-change snapshot, deletions, anomalies.

**STOP — Approval required**
> Reply: `APPROVED: BE-05` or `CHANGES: ...`

---

## BE-06 — Lua Commit v1 (Atomic: fencing + recency + seq + indices + state)
**Goal:** Single Redis EVAL applies entire write plan atomically and fenced by epoch.  
**Steps**
1. Load current `epoch` from Redis (or pass in from leader election) and provide to Lua.
2. In Lua:
   - Verify `epoch` matches `bp:leader:epoch`; else return `FENCED`.
   - Verify `msgTs > bp:recency:{topic}:{producerId}`; else return `DROPPED_RECENCY`.
   - Allocate SEQs: `base = INCRBY bp:seq:{topic} nChanges`; assign `seq = base - nChanges + i`.
   - Apply upserts:
     - `HSET bp:state:v:{topic} itemId version`
     - `HSET bp:state:j:{topic} itemId dataJson`
     - `ZADD bp:ch:up:{topic} seq itemId`
     - `HSET bp:prodver:{topic}:{producerId} itemId version`
   - Apply deletes:
     - `HDEL bp:state:v:{topic} itemId`
     - `HDEL bp:state:j:{topic} itemId`
     - `ZADD bp:ch:del:{topic} seq itemId`
     - `HDEL bp:prodver:{topic}:{producerId} itemId`
   - Update recency key: `SET bp:recency:{topic}:{producerId} msgTs`
3. Return `{status, firstSeq, lastSeq, appliedUpserts, appliedDeletes}`.

**DoD**
- One EVAL is sufficient for a snapshot (no partial visibility).
- Stale epoch returns `FENCED` and changes nothing.
- Recency violations return `DROPPED_RECENCY` and change nothing.

**Verification**
- Testcontainers integration:
  - Write with epoch=1 works; epoch=0 is fenced.
  - Out-of-order msgTs does not mutate state hashes or zsets.

**STOP — Approval required**
> Reply: `APPROVED: BE-06` or `CHANGES: ...`

---

## BE-07 — Commit Execution Wrapper (EVALSHA + retries + error mapping)
**Goal:** Make commit path stable and production-safe.  
**Steps**
1. Implement script loading + `EVALSHA` caching; reload on `NOSCRIPT`.
2. Map Lua outputs to internal results: OK / DROPPED / FENCED.
3. Implement safe retry policy:
   - Retry on transient Redis connection errors.
   - Do NOT retry on DROPPED/FENCED.
4. Emit metrics:
   - `bp_commit_latency_ms` histogram
   - `bp_commit_total{status}`
   - `bp_commit_fail_total{cause}`

**DoD**
- Commit wrapper handles `NOSCRIPT` automatically.
- Metrics include latency + statuses.

**Verification**
- Unit test for `NOSCRIPT` branch using mocked Redis.
- Integration: kill Redis briefly and observe bounded retries + recovery.

**STOP — Approval required**
> Reply: `APPROVED: BE-07` or `CHANGES: ...`

---

## BE-08 — KOSERVER `/changes` (Read Path)
**Goal:** Implement pull endpoint to return changed items and deletions since `sinceSeq`.  
**Steps**
1. Read `minRetained = GET bp:seq:min:{topic}` (default 0 if missing).
2. If `sinceSeq < minRetained` → return `fullResyncRequired=true` and `newSeq=sinceSeq`.
3. Query upserts: `ZRANGEBYSCORE bp:ch:up:{topic} (sinceSeq, +inf] LIMIT 0 limit WITHSCORES`.
4. Query deletes similarly from `bp:ch:del:{topic}`.
5. Fetch payload:
   - `HMGET bp:state:v:{topic} itemIds`
   - `HMGET bp:state:j:{topic} itemIds`
6. Resolve conflicts (same itemId in up+del window) by higher seq.
7. Compute `newSeq` as max seq returned (see BE-09).

**DoD**
- Endpoint returns correct items/deleted for a known dataset.
- No heavy commands (no HGETALL). Only keyed HMGET for selected ids.

**Verification**
- Integration: seed Redis with known indices + state, call `/changes` and assert response.

**STOP — Approval required**
> Reply: `APPROVED: BE-08` or `CHANGES: ...`

---

## BE-09 — Pagination Safety Rule (newSeq = max returned score)
**Goal:** Prevent gaps when `limit` truncates results.  
**Steps**
1. Compute `newSeq = max(seq returned among upserts and deletes)`.
2. If nothing returned, `newSeq = sinceSeq`.
3. Clients loop using returned `newSeq`.

**DoD**
- With 25 changes and `limit=10`, 3 calls return all 25 changes with no skipped ids.

**Verification**
- Integration test: generate sequential changes 1..25; loop calls and ensure set coverage=25.

**STOP — Approval required**
> Reply: `APPROVED: BE-09` or `CHANGES: ...`

---

## BE-10 — Retention Job (Leader-only) + minRetained watermark
**Goal:** Trim old seq entries and advance `bp:seq:min:{topic}`.  
**Steps**
1. Run on leader only (epoch check or leader-election local).
2. Compute `minKeep = currentSeq - RETAIN_WINDOW` (bounded at 0).
3. `ZREMRANGEBYSCORE` on both indices for `(-inf, minKeep]`.
4. `SET bp:seq:min:{topic} minKeep` (monotonic).

**DoD**
- After retention, calls with `sinceSeq < minKeep` return `fullResyncRequired=true`.
- Watermark never decreases.

**Verification**
- Integration: create seq 1..100, retain 50, run job => minKeep=50 and old entries trimmed.

**STOP — Approval required**
> Reply: `APPROVED: BE-10` or `CHANGES: ...`

---

## BE-11 — End-to-End Integration Suite (Testcontainers)
**Goal:** Lock correctness via reproducible tests.  
**Steps**
1. Testcontainers for Redis + (optional) ZK + ActiveMQ.
2. Cases: fencing, recency drop, atomicity, pagination, retention.
3. Wire into CI.

**DoD**
- ≥ 15 integration cases pass in CI.
- Tests run < 5 minutes (target).

**Verification**
- CI pipeline log + artifacts.

**STOP — Approval required**
> Reply: `APPROVED: BE-11` or `CHANGES: ...`

---

## BE-12 — Guardrails (payload caps + per-commit caps)
**Goal:** Protect Redis and the service from pathological payloads.  
**Steps**
1. Enforce max `dataJson` bytes; policy: reject snapshot or item (config-driven).
2. Enforce max changes per commit and/or max total bytes passed to Lua.
3. Emit metrics: `bp_rejected_total{reason}`.
4. Document defaults in config README.

**DoD**
- Oversized payload deterministically rejected and visible in metrics/logs.
- Normal payloads unaffected.

**Verification**
- Unit tests for size checks.
- Integration smoke with near-limit payload.

**STOP — Approval required**
> Reply: `APPROVED: BE-12` or `CHANGES: ...`
