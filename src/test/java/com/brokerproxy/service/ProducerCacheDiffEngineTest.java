package com.brokerproxy.service;

import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.SnapshotItem;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ProducerCacheDiffEngine#computeDiff} — pure logic, no Redis.
 *
 * <p>Exercises multi-item producer scenarios (headsets and conferences) where a single
 * snapshot can contain hundreds of items. The engine is constructed without a real
 * {@link io.vertx.redis.client.RedisAPI}; we call the package-private
 * {@code computeDiff(Snapshot, Map)} directly.
 *
 * <h3>Test scenarios</h3>
 * <ol>
 *   <li>Large snapshot (500 items), 0 changes → empty WritePlan (spec requirement)</li>
 *   <li>Large snapshot (500 items), exactly 1 changed item → 1 upsert, 0 deletes</li>
 *   <li>Items in cache absent from snapshot → DELETE each missing item</li>
 *   <li>Empty cache (first snapshot for producer) → all items upserted</li>
 *   <li>Version anomaly in multi-item snapshot → anomalous items skipped</li>
 *   <li>Mix of upserts, deletes, NOOPs, and anomalies in one snapshot</li>
 * </ol>
 */
class ProducerCacheDiffEngineTest {

    private static final String TOPIC       = "conferences";
    private static final String PRODUCER_ID = "conf-prod-1";

    private ProducerCacheDiffEngine engine;

    @BeforeEach
    void setUp() {
        engine = new ProducerCacheDiffEngine(null, "bp");
    }

    // ---- Spec requirement: 500 items, 0 changes → NOOP -------------------------

    @Test
    @DisplayName("500-item snapshot with zero changes → empty WritePlan (full NOOP)")
    void large_snapshot_no_changes_is_noop() {
        Map<String, Long> cache    = buildCache(500, 10L);
        Snapshot          snapshot = buildSnapshot(TOPIC, 500, 10L);

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.isEmpty()).isTrue();
        assertThat(plan.upserts()).isEmpty();
        assertThat(plan.deletes()).isEmpty();
    }

    // ---- Spec requirement: 500 items, 1 change → 1 upsert ----------------------

    @Test
    @DisplayName("500-item snapshot with exactly 1 changed item → 1 upsert, 0 deletes")
    void large_snapshot_one_change_yields_single_upsert() {
        Map<String, Long> cache = buildCache(500, 10L);
        // Bump version of item-250 to trigger one upsert
        cache.put("item-250", 10L);  // same as others, so we need to bump in snapshot

        Snapshot snapshot = buildSnapshotWithOneChanged(TOPIC, 500, 10L, "item-250", 11L);

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.upserts()).hasSize(1);
        assertThat(plan.upserts().get(0).itemId()).isEqualTo("item-250");
        assertThat(plan.upserts().get(0).version()).isEqualTo(11L);
        assertThat(plan.deletes()).isEmpty();
    }

    // ---- Deletions: items in cache but absent from snapshot ----------------------

    @Test
    @DisplayName("Items in cache absent from snapshot → DELETE each missing item")
    void items_absent_from_snapshot_are_deleted() {
        // Cache has 5 items; snapshot only sends 3 → 2 should be deleted
        Map<String, Long> cache = new HashMap<>();
        cache.put("item-1", 1L);
        cache.put("item-2", 1L);
        cache.put("item-3", 1L);
        cache.put("item-gone-A", 1L);
        cache.put("item-gone-B", 1L);

        Snapshot snapshot = snapFromItems(TOPIC, new String[]{"item-1", "item-2", "item-3"},
                new long[]{1L, 1L, 1L});

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.deletes()).hasSize(2);
        assertThat(plan.deletes()).containsExactlyInAnyOrder("item-gone-A", "item-gone-B");
        // Remaining items are unchanged → no upserts
        assertThat(plan.upserts()).isEmpty();
    }

    // ---- First snapshot: empty cache → all items upserted -----------------------

    @Test
    @DisplayName("Empty cache (first snapshot for this producer) → all items upserted")
    void empty_cache_yields_all_upserts() {
        int      count    = 10;
        Snapshot snapshot = buildSnapshot("headsets", count, 1L);

        WritePlan plan = engine.computeDiff(snapshot, Map.of());

        assertThat(plan.upserts()).hasSize(count);
        assertThat(plan.deletes()).isEmpty();
    }

    // ---- Version anomaly --------------------------------------------------------

    @Test
    @DisplayName("newVersion < storedVersion → anomalous items skipped, others applied normally")
    void version_anomaly_items_are_skipped() {
        Map<String, Long> cache = new HashMap<>();
        cache.put("item-ok",      5L);
        cache.put("item-anomaly", 10L);

        // item-ok bumped (should upsert), item-anomaly regressed (should skip)
        Snapshot snapshot = snapFromItems(TOPIC,
                new String[]{"item-ok", "item-anomaly"},
                new long[]{6L, 9L});

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.upserts()).hasSize(1);
        assertThat(plan.upserts().get(0).itemId()).isEqualTo("item-ok");

        // Anomalous item must NOT appear in upserts or deletes
        assertThat(plan.upserts()).noneMatch(u -> u.itemId().equals("item-anomaly"));
        assertThat(plan.deletes()).doesNotContain("item-anomaly");
    }

    // ---- Mixed plan: upserts + deletes + NOOPs + anomaly ------------------------

    @Test
    @DisplayName("Mixed snapshot: upsert + delete + NOOP + anomaly all handled correctly")
    void mixed_plan_is_computed_correctly() {
        Map<String, Long> cache = new HashMap<>();
        cache.put("item-noop",    3L);  // same version → NOOP
        cache.put("item-upsert",  3L);  // version bumped → UPSERT
        cache.put("item-deleted", 3L);  // absent from snapshot → DELETE
        cache.put("item-anomaly", 3L);  // version regressed → ANOMALY (skip)

        Snapshot snapshot = snapFromItems(TOPIC,
                new String[]{"item-noop", "item-upsert", "item-anomaly", "item-new"},
                new long[]{3L, 4L, 2L, 1L});

        WritePlan plan = engine.computeDiff(snapshot, cache);

        // item-upsert (version 3→4) + item-new (not in cache)
        assertThat(plan.upserts()).hasSize(2);
        assertThat(plan.upserts()).extracting(SnapshotItem::itemId)
                .containsExactlyInAnyOrder("item-upsert", "item-new");

        // item-deleted (not in snapshot)
        assertThat(plan.deletes()).containsExactly("item-deleted");

        // NOOP and anomaly items must not appear in upserts or deletes
        assertThat(plan.upserts()).noneMatch(u -> u.itemId().equals("item-noop"));
        assertThat(plan.upserts()).noneMatch(u -> u.itemId().equals("item-anomaly"));
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Builds a cache of {@code count} items all at the same {@code version}.
     * ItemIds are: "item-0", "item-1", …, "item-{count-1}".
     */
    private static Map<String, Long> buildCache(int count, long version) {
        Map<String, Long> cache = new HashMap<>();
        IntStream.range(0, count).forEach(i -> cache.put("item-" + i, version));
        return cache;
    }

    /**
     * Builds a Snapshot with {@code count} items all at the same {@code version}.
     */
    private static Snapshot buildSnapshot(String topic, int count, long version) {
        JsonArray items = new JsonArray();
        IntStream.range(0, count).forEach(i ->
                items.add(new JsonObject()
                        .put("itemId",   "item-" + i)
                        .put("version",  version)
                        .put("dataJson", "{}")));
        return Snapshot.fromJson(new JsonObject()
                .put("topic",      topic)
                .put("producerId", PRODUCER_ID)
                .put("msgTs",      1000L)
                .put("items",      items));
    }

    /**
     * Builds a snapshot with {@code count} items all at {@code baseVersion} except
     * one item ({@code changedId}) which is set to {@code changedVersion}.
     */
    private static Snapshot buildSnapshotWithOneChanged(String topic, int count,
                                                         long baseVersion,
                                                         String changedId,
                                                         long changedVersion) {
        JsonArray items = new JsonArray();
        IntStream.range(0, count).forEach(i -> {
            String itemId  = "item-" + i;
            long   version = itemId.equals(changedId) ? changedVersion : baseVersion;
            items.add(new JsonObject()
                    .put("itemId",   itemId)
                    .put("version",  version)
                    .put("dataJson", "{}"));
        });
        return Snapshot.fromJson(new JsonObject()
                .put("topic",      topic)
                .put("producerId", PRODUCER_ID)
                .put("msgTs",      1000L)
                .put("items",      items));
    }

    /** Builds a snapshot from parallel itemId and version arrays. */
    private static Snapshot snapFromItems(String topic, String[] ids, long[] versions) {
        JsonArray items = new JsonArray();
        for (int i = 0; i < ids.length; i++) {
            items.add(new JsonObject()
                    .put("itemId",   ids[i])
                    .put("version",  versions[i])
                    .put("dataJson", "{}"));
        }
        return Snapshot.fromJson(new JsonObject()
                .put("topic",      topic)
                .put("producerId", PRODUCER_ID)
                .put("msgTs",      1000L)
                .put("items",      items));
    }
}
