package com.brokerproxy.service;

import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.SnapshotItem;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Computes the {@link WritePlan} for the {@code computers} topic.
 *
 * <h3>Computers invariant</h3>
 * Each producer sends exactly one item per snapshot (single-item producer).
 * The baseline is stored in {@code bp:prodver:computers:{producerId}} as a
 * HASH with a single entry: {@code itemId → version}.
 *
 * <h3>Diff rules (evaluated per item in the incoming snapshot)</h3>
 * <ol>
 *   <li>Item not in cache (first snapshot from this producer) → UPSERT</li>
 *   <li>Same itemId, same version → NOOP</li>
 *   <li>Same itemId, higher version → UPSERT</li>
 *   <li>Different itemId (itemId changed) → DELETE old + UPSERT new</li>
 *   <li>newVersion &lt; storedVersion → VERSION_ANOMALY: log warn, count metric, skip item</li>
 * </ol>
 *
 * <h3>Redis key</h3>
 * <pre>
 *   bp:prodver:{topic}:{producerId}  →  Hash (itemId → version as decimal long)
 * </pre>
 * Shared key pattern with the headsets/conferences diff engine (BE-05) — consistent
 * across all topics and used as the sole source of truth for the Lua commit (BE-06).
 * No TTL — keys are permanent per active producer; negligible size (one field each
 * for computers, many fields for headsets/conferences).
 */
public class ComputersDiffEngine {

    private static final Logger log = LoggerFactory.getLogger(ComputersDiffEngine.class);

    private final RedisAPI redis;
    private final String   prefix;

    public ComputersDiffEngine(RedisAPI redis, String prefix) {
        this.redis  = redis;
        this.prefix = prefix;
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Reads {@code bp:prodver:{topic}:{producerId}} and computes the
     * {@link WritePlan} for this snapshot.
     *
     * <p>All I/O is non-blocking. The returned {@link Future} completes on the
     * Vert.x event loop — no blocking calls inside.
     */
    public Future<WritePlan> diff(Snapshot snapshot) {
        String cacheKey = cacheKey(snapshot.topic(), snapshot.producerId());

        return redis.hgetall(cacheKey)
                .map(response -> {
                    Map<String, Long> oldVersions = parseHgetall(response);
                    return computeDiff(snapshot, oldVersions);
                })
                .onFailure(err -> log.error(
                        "event=computers_diff.cache_read_error topic={} producerId={} error=\"{}\"",
                        snapshot.topic(), snapshot.producerId(), err.getMessage()));
    }

    // ---- Package-private for unit testing ---------------------------------------

    /**
     * Pure diff computation — no I/O.
     *
     * <p>Exposed package-private so unit tests can exercise all golden cases
     * without an actual Redis instance.
     *
     * @param snapshot      incoming snapshot (single-item for computers)
     * @param oldVersions   current contents of {@code bp:prodver} ({@code itemId → version})
     * @return the computed {@link WritePlan}
     */
    WritePlan computeDiff(Snapshot snapshot, Map<String, Long> oldVersions) {
        List<SnapshotItem> upserts = new ArrayList<>();
        List<String>       deletes = new ArrayList<>();

        String topic      = snapshot.topic();
        String producerId = snapshot.producerId();

        // Index new items by itemId for O(1) lookup during deletion scan
        Map<String, SnapshotItem> newById = new HashMap<>();
        for (SnapshotItem item : snapshot.items()) {
            newById.put(item.itemId(), item);
        }

        // --- Pass 1: detect deletions (old items absent from the new snapshot) ---
        for (String oldItemId : oldVersions.keySet()) {
            if (!newById.containsKey(oldItemId)) {
                log.debug("event=computers_diff.delete topic={} producerId={} itemId={}",
                        topic, producerId, oldItemId);
                deletes.add(oldItemId);
            }
        }

        // --- Pass 2: classify each incoming item ---
        for (SnapshotItem newItem : snapshot.items()) {
            Long oldVersion = oldVersions.get(newItem.itemId());

            if (oldVersion == null) {
                // New item — not previously seen for this producer
                log.debug("event=computers_diff.upsert_new topic={} producerId={} "
                                + "itemId={} version={}",
                        topic, producerId, newItem.itemId(), newItem.version());
                upserts.add(newItem);

            } else if (newItem.version() > oldVersion) {
                // Higher version — apply the update
                log.debug("event=computers_diff.upsert_higher topic={} producerId={} "
                                + "itemId={} oldVersion={} newVersion={}",
                        topic, producerId, newItem.itemId(), oldVersion, newItem.version());
                upserts.add(newItem);

            } else if (newItem.version() == oldVersion) {
                // Exact match — no-op for this item
                log.debug("event=computers_diff.noop_item topic={} producerId={} "
                                + "itemId={} version={}",
                        topic, producerId, newItem.itemId(), newItem.version());

            } else {
                // newVersion < storedVersion → anomaly
                log.warn("event=computers_diff.version_anomaly topic={} producerId={} "
                                + "itemId={} storedVersion={} incomingVersion={}",
                        topic, producerId, newItem.itemId(), oldVersion, newItem.version());
                BrokerMetrics.versionAnomaly(topic);
                // Policy: skip — do not apply a regression in version
            }
        }

        // --- Build and log the plan ---
        WritePlan plan = new WritePlan(topic, producerId, snapshot.msgTs(),
                List.copyOf(upserts), List.copyOf(deletes));

        if (plan.isEmpty()) {
            log.info("event=computers_diff.noop topic={} producerId={} msgTs={}",
                    topic, producerId, snapshot.msgTs());
            BrokerMetrics.snapshotNoop(topic);
        } else {
            log.info("event=computers_diff.plan_ready topic={} producerId={} msgTs={} {}",
                    topic, producerId, snapshot.msgTs(), plan.summary());
            plan.upserts().forEach(u -> BrokerMetrics.itemUpsert(topic));
            plan.deletes().forEach(d -> BrokerMetrics.itemDelete(topic));
        }

        return plan;
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Parses a Redis HGETALL {@link Response} into a plain {@code Map<String, Long>}.
     *
     * <p>HGETALL returns a flat bulk-string array: [field0, val0, field1, val1, …].
     * {@code response.size()} is 2 × (number of fields). An empty or absent key
     * returns size 0 (not null).
     *
     * <p>Note: {@code Response.keys()} is NOT available in Vert.x 4.5.x — use
     * index-based iteration over the flat array.
     */
    private static Map<String, Long> parseHgetall(Response response) {
        Map<String, Long> result = new HashMap<>();
        if (response == null || response.size() == 0) {
            return result;
        }
        // Flat array: [field0, val0, field1, val1, ...]
        for (int i = 0; i < response.size() - 1; i += 2) {
            String field   = response.get(i).toString();
            long   version = Long.parseLong(response.get(i + 1).toString());
            result.put(field, version);
        }
        return result;
    }

    /** {@code bp:prodver:{topic}:{producerId}} */
    private String cacheKey(String topic, String producerId) {
        return prefix + ":prodver:" + topic + ":" + producerId;
    }
}
