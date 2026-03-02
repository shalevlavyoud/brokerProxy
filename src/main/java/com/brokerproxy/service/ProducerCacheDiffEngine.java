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
 * General-purpose diff engine for all topics that use the
 * {@code bp:prodver:{topic}:{producerId}} Hash as their baseline.
 *
 * <p>Handles {@code headsets} and {@code conferences} (multi-item producers)
 * as well as {@code computers} (single-item producer) via delegation from
 * {@link ComputersDiffEngine}.
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>Read {@code HGETALL bp:prodver:{topic}:{producerId}} → {@code oldVersions}</li>
 *   <li>For each item in the incoming snapshot:
 *     <ul>
 *       <li>Not in cache → <b>UPSERT</b> (new item)</li>
 *       <li>Same version → <b>NOOP</b></li>
 *       <li>Higher version → <b>UPSERT</b></li>
 *       <li>Lower version → <b>VERSION_ANOMALY</b>: log warn, count metric, skip</li>
 *     </ul>
 *   </li>
 *   <li>Items in cache but absent from snapshot → <b>DELETE</b>
 *       (ownership transfer is modelled as DELETE + UPSERT)</li>
 * </ol>
 *
 * <h3>Redis key</h3>
 * <pre>
 *   bp:prodver:{topic}:{producerId}  →  Hash (itemId → version as decimal long)
 * </pre>
 * No TTL — permanent per active producer. Size is proportional to the number of
 * items that producer currently owns (may be large for conferences: ~500 entries).
 *
 * <h3>Atomicity note</h3>
 * This class only <em>reads</em> from Redis. The Lua commit script (BE-06) will
 * apply the resulting {@link WritePlan} atomically, including updating this hash.
 */
public class ProducerCacheDiffEngine {

    private static final Logger log = LoggerFactory.getLogger(ProducerCacheDiffEngine.class);

    private final RedisAPI redis;
    private final String   prefix;

    public ProducerCacheDiffEngine(RedisAPI redis, String prefix) {
        this.redis  = redis;
        this.prefix = prefix;
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Reads the producer-version cache and computes the {@link WritePlan}.
     *
     * <p>All I/O is non-blocking. The returned {@link Future} completes on the
     * Vert.x event loop — no blocking calls inside.
     */
    public Future<WritePlan> diff(Snapshot snapshot) {
        String cacheKey = cacheKey(snapshot.topic(), snapshot.producerId());

        return redis.hgetall(cacheKey)
                .map(response -> computeDiff(snapshot, parseHgetall(response)))
                .onFailure(err -> log.error(
                        "event=prodver_diff.cache_read_error topic={} producerId={} error=\"{}\"",
                        snapshot.topic(), snapshot.producerId(), err.getMessage()));
    }

    // ---- Package-private for unit testing ---------------------------------------

    /**
     * Pure diff computation — no I/O.
     *
     * <p>Exposed package-private so unit tests can exercise all golden cases
     * without an actual Redis instance.
     *
     * @param snapshot    incoming snapshot
     * @param oldVersions current contents of {@code bp:prodver} ({@code itemId → version})
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
                log.debug("event=prodver_diff.delete topic={} producerId={} itemId={}",
                        topic, producerId, oldItemId);
                deletes.add(oldItemId);
            }
        }

        // --- Pass 2: classify each incoming item ---
        for (SnapshotItem newItem : snapshot.items()) {
            Long oldVersion = oldVersions.get(newItem.itemId());

            if (oldVersion == null) {
                // New item — not previously seen for this producer
                log.debug("event=prodver_diff.upsert_new topic={} producerId={} "
                                + "itemId={} version={}",
                        topic, producerId, newItem.itemId(), newItem.version());
                upserts.add(newItem);

            } else if (newItem.version() > oldVersion) {
                // Higher version — apply the update
                log.debug("event=prodver_diff.upsert_higher topic={} producerId={} "
                                + "itemId={} oldVersion={} newVersion={}",
                        topic, producerId, newItem.itemId(), oldVersion, newItem.version());
                upserts.add(newItem);

            } else if (newItem.version() == oldVersion) {
                // Exact match — no-op for this item
                log.debug("event=prodver_diff.noop_item topic={} producerId={} "
                                + "itemId={} version={}",
                        topic, producerId, newItem.itemId(), newItem.version());

            } else {
                // newVersion < storedVersion → anomaly
                log.warn("event=prodver_diff.version_anomaly topic={} producerId={} "
                                + "itemId={} storedVersion={} incomingVersion={}",
                        topic, producerId, newItem.itemId(), oldVersion, newItem.version());
                BrokerMetrics.versionAnomaly(topic);
                // Policy: skip — do not apply a version regression
            }
        }

        // --- Build and log the plan ---
        WritePlan plan = new WritePlan(topic, producerId, snapshot.msgTs(),
                List.copyOf(upserts), List.copyOf(deletes));

        if (plan.isEmpty()) {
            log.info("event=prodver_diff.noop topic={} producerId={} msgTs={} itemCount={}",
                    topic, producerId, snapshot.msgTs(), snapshot.items().size());
            BrokerMetrics.snapshotNoop(topic);
        } else {
            log.info("event=prodver_diff.plan_ready topic={} producerId={} msgTs={} "
                            + "itemCount={} {}",
                    topic, producerId, snapshot.msgTs(), snapshot.items().size(),
                    plan.summary());
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
     * {@code Response.keys()} is NOT available in Vert.x 4.5.x; use index-based
     * iteration with {@code i += 2}.
     */
    static Map<String, Long> parseHgetall(Response response) {
        Map<String, Long> result = new HashMap<>();
        if (response == null || response.size() == 0) {
            return result;
        }
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
