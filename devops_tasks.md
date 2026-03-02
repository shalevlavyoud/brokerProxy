# DevOps Tasks — Broker Proxy + KOSERVER (Sequential w/ Approval Gates)

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
- Approve: `APPROVED: OPS-XX`
- Request changes: `CHANGES: ...`

The agent may not start the next task until approval is received.

---

## OPS-01 — Dockerfiles + Local docker-compose Stack
**Goal:** One-command local stack for end-to-end dev: Redis(+Sentinel), ZK, ActiveMQ, Broker Proxy, KOSERVER.  
**Steps**
1. Write Dockerfiles for Broker Proxy + KOSERVER.
2. Create docker-compose with:
   - Redis primary + replica + Sentinel
   - ZooKeeper
   - ActiveMQ
   - Prometheus (optional but recommended)
3. Provide `.env` + sample configs for topics and redis endpoints.

**DoD**
- `docker compose up` brings everything healthy.
- Basic smoke: publish snapshot → Redis populated → `/changes` returns results.

**Verification**
- Run locally and capture logs + curl `/health` and `/changes`.

**STOP — Approval required**
> Reply: `APPROVED: OPS-01` or `CHANGES: ...`

---

## OPS-02 — CI Pipeline (Build + Unit + Integration)
**Goal:** Make build reproducible and prevent regressions.  
**Steps**
1. CI job: build images, run unit tests.
2. CI job: run integration tests (Testcontainers).
3. Cache dependencies for faster runs.
4. Publish artifacts/images tagged with commit SHA.

**DoD**
- PR cannot merge unless CI green.
- Integration suite runs automatically.

**Verification**
- CI run log shows all stages passing.

**STOP — Approval required**
> Reply: `APPROVED: OPS-02` or `CHANGES: ...`

---

## OPS-03 — Kubernetes Manifests (Helm/Kustomize) + Probes
**Goal:** Deploy to staging k3s/OKD with sane defaults.  
**Steps**
1. Create Helm chart or Kustomize overlays:
   - Deployments for Broker Proxy and KOSERVER
   - Services, ConfigMaps, Secrets
2. Add readiness/liveness probes (`/health`).
3. Add resource requests/limits and (optional) anti-affinity.

**DoD**
- `kubectl apply` (or `helm install`) results in healthy pods.
- Rollout completes with no restarts.

**Verification**
- `kubectl rollout status` + `kubectl get pods` shows Ready.

**STOP — Approval required**
> Reply: `APPROVED: OPS-03` or `CHANGES: ...`

---

## OPS-04 — Redis Sentinel Reliability Settings + Failover Runbook
**Goal:** Protect against acknowledged writes that are not replicated and document failover operations.  
**Steps**
1. Set/validate Redis configs:
   - `min-replicas-to-write`
   - `min-replicas-max-lag`
2. Ensure clients have reconnect strategy and timeouts.
3. Write a failover runbook and checklist.

**DoD**
- Configs are explicit and versioned.
- Runbook exists and is executable.

**Verification**
- Planned failover drill in staging passes checklist.

**STOP — Approval required**
> Reply: `APPROVED: OPS-04` or `CHANGES: ...`

---

## OPS-05 — Observability Minimal: Prometheus scrape + Alerts
**Goal:** Minimal but real visibility.  
**Steps**
1. Deploy Prometheus scrape configs for both services.
2. Provide Grafana dashboards or PromQL snippets:
   - commit latency p99
   - commit statuses OK/DROPPED/FENCED
   - `/changes` latency p99
   - drop reasons
3. Add alerts for high latency and high drop ratio.

**DoD**
- Metrics are scraped successfully in staging.
- At least 3 alerts exist and can be triggered by injection.

**Verification**
- Trigger injected conditions and confirm alert fires.

**STOP — Approval required**
> Reply: `APPROVED: OPS-05` or `CHANGES: ...`

---

## OPS-06 — Safe Rollout Strategy (Shadow prefix + Flag + Rollback)
**Goal:** No-drama rollout with fast rollback.  
**Steps**
1. Deploy using a shadow Redis prefix (e.g., `bp2:*`) in parallel.
2. Add runtime flag/config to switch KOSERVER read prefix.
3. Document rollback: flip flag back.

**DoD**
- Shadow mode runs without impacting current consumers.
- Switch/rollback takes < 1 minute.

**Verification**
- Manual drill: switch to bp2 and back while monitoring `/changes`.

**STOP — Approval required**
> Reply: `APPROVED: OPS-06` or `CHANGES: ...`

---

## OPS-07 — Redis Exporter + Slowlog Guardrails
**Goal:** Catch hot keys / slow Lua / forbidden patterns early.  
**Steps**
1. Deploy Redis exporter.
2. Enable and monitor Redis slowlog.
3. Add alerts on slowlog spikes and operational note: no `HGETALL` on state hashes.

**DoD**
- Exporter metrics visible; slowlog is collected/queried.
- Alert triggers on controlled slowlog injection.

**Verification**
- Inject slow command and confirm alert + runbook action.

**STOP — Approval required**
> Reply: `APPROVED: OPS-07` or `CHANGES: ...`
