#!/usr/bin/env bash
# =============================================================================
# Smoke Test — Broker Proxy Local Stack
# Verifies: Redis Sentinel, ZooKeeper, ActiveMQ, Broker Proxy /health, KOSERVER /changes
#
# Usage:
#   chmod +x scripts/smoke-test.sh
#   ./scripts/smoke-test.sh
#
# Requirements: docker, redis-cli (or use the container), curl, nc
# =============================================================================

set -euo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${YELLOW}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
fail()  { echo -e "${RED}[FAIL]${NC}  $*"; exit 1; }

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PRIMARY_PORT="${REDIS_PRIMARY_PORT:-6379}"
REDIS_SENTINEL_PORT="${REDIS_SENTINEL_1_PORT:-26379}"
ZK_HOST="${ZK_HOST:-localhost}"
ZK_PORT="${ZK_PORT:-2181}"
ACTIVEMQ_HOST="${ACTIVEMQ_HOST:-localhost}"
ACTIVEMQ_PORT="${ACTIVEMQ_OPENWIRE_PORT:-61616}"
ACTIVEMQ_CONSOLE="${ACTIVEMQ_CONSOLE_PORT:-8161}"
BP_HOST="${BP_HOST:-localhost}"
BP_PORT="${BP_PORT:-8080}"
KOSERVER_HOST="${KOSERVER_HOST:-localhost}"
KOSERVER_PORT="${KOSERVER_PORT:-8081}"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Broker Proxy Smoke Test"
echo "═══════════════════════════════════════════════════════════════"

# ─────────────────────────────────────────────────────────────────────────────
# 1. Redis Primary — PING
# ─────────────────────────────────────────────────────────────────────────────
info "1/7  Redis Primary PING (${REDIS_HOST}:${REDIS_PRIMARY_PORT})"
REDIS_PING=$(docker exec redis-primary redis-cli ping 2>/dev/null || true)
if [ "$REDIS_PING" = "PONG" ]; then
  ok "Redis Primary responded: PONG"
else
  fail "Redis Primary did not respond to PING (got: '${REDIS_PING}')"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2. Redis Sentinel — PING + master check
# ─────────────────────────────────────────────────────────────────────────────
info "2/7  Redis Sentinel (port ${REDIS_SENTINEL_PORT}) — checking master"
SENTINEL_MASTER=$(docker exec redis-sentinel-1 redis-cli -p 26379 sentinel get-master-addr-by-name mymaster 2>/dev/null || true)
if [ -n "$SENTINEL_MASTER" ]; then
  ok "Sentinel master address: $(echo "$SENTINEL_MASTER" | tr '\n' ' ')"
else
  fail "Sentinel could not resolve master 'mymaster'"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 3. Redis Replica — replication check
# ─────────────────────────────────────────────────────────────────────────────
info "3/7  Redis Replica replication status"
REPL_STATUS=$(docker exec redis-replica redis-cli info replication 2>/dev/null | grep role || true)
if echo "$REPL_STATUS" | grep -q "slave\|replica"; then
  ok "Replica role confirmed: ${REPL_STATUS}"
else
  fail "Redis Replica does not appear to be replicating (got: '${REPL_STATUS}')"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4. ZooKeeper — ruok check
# ─────────────────────────────────────────────────────────────────────────────
info "4/7  ZooKeeper ruok (${ZK_HOST}:${ZK_PORT})"
ZK_RESP=$(echo ruok | nc -w 2 "${ZK_HOST}" "${ZK_PORT}" 2>/dev/null || true)
if [ "$ZK_RESP" = "imok" ]; then
  ok "ZooKeeper responded: imok"
else
  fail "ZooKeeper did not respond with 'imok' (got: '${ZK_RESP}')"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 5. ActiveMQ — Web console reachable
# ─────────────────────────────────────────────────────────────────────────────
info "5/7  ActiveMQ Web Console (http://${ACTIVEMQ_HOST}:${ACTIVEMQ_CONSOLE}/admin)"
AMQ_STATUS=$(curl -sf -u admin:admin -o /dev/null -w "%{http_code}" \
  "http://${ACTIVEMQ_HOST}:${ACTIVEMQ_CONSOLE}/admin" 2>/dev/null || echo "000")
if [ "$AMQ_STATUS" = "200" ]; then
  ok "ActiveMQ Web Console reachable (HTTP 200)"
else
  fail "ActiveMQ Web Console returned HTTP ${AMQ_STATUS}"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 6. Broker Proxy — /health
# ─────────────────────────────────────────────────────────────────────────────
info "6/7  Broker Proxy /health (http://${BP_HOST}:${BP_PORT}/health)"
BP_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
  "http://${BP_HOST}:${BP_PORT}/health" 2>/dev/null || echo "000")
if [ "$BP_STATUS" = "200" ]; then
  BP_BODY=$(curl -sf "http://${BP_HOST}:${BP_PORT}/health" 2>/dev/null || echo "{}")
  ok "Broker Proxy healthy (HTTP 200): ${BP_BODY}"
else
  fail "Broker Proxy /health returned HTTP ${BP_STATUS} (is the backend deployed?)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 7. KOSERVER — /changes for all topics
# ─────────────────────────────────────────────────────────────────────────────
info "7/7  KOSERVER /changes for topics: computers, headsets, conferences"
for TOPIC in computers headsets conferences; do
  KO_STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
    "http://${KOSERVER_HOST}:${KOSERVER_PORT}/changes?topic=${TOPIC}&sinceSeq=0&limit=10" \
    2>/dev/null || echo "000")
  if [ "$KO_STATUS" = "200" ]; then
    KO_BODY=$(curl -sf \
      "http://${KOSERVER_HOST}:${KOSERVER_PORT}/changes?topic=${TOPIC}&sinceSeq=0&limit=10" \
      2>/dev/null || echo "{}")
    ok "  [${TOPIC}] HTTP 200 — ${KO_BODY}"
  else
    fail "  [${TOPIC}] /changes returned HTTP ${KO_STATUS}"
  fi
done

# ─────────────────────────────────────────────────────────────────────────────
# BONUS: Quick Redis key dump (to verify broker-proxy wrote state)
# ─────────────────────────────────────────────────────────────────────────────
echo ""
info "BONUS  Redis key summary (bp:* namespace)"
docker exec redis-primary redis-cli --scan --pattern 'bp:*' 2>/dev/null | head -30 | while read -r key; do
  TYPE=$(docker exec redis-primary redis-cli type "$key" 2>/dev/null)
  echo "  ${key}  [${TYPE}]"
done

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  All smoke tests passed! ✅${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "  Prometheus:    http://localhost:${PROMETHEUS_PORT:-9090}"
echo "  ActiveMQ:      http://localhost:${ACTIVEMQ_CONSOLE:-8161}/admin  (admin/admin)"
echo "  Broker Proxy:  http://localhost:${BP_PORT:-8080}/health"
echo "  KOSERVER:      http://localhost:${KOSERVER_PORT:-8081}/health"
echo ""
