package com.brokerproxy.service;

import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.SnapshotItem;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ComputersDiffEngine#computeDiff} — pure logic, no Redis.
 *
 * <p>The diff engine is constructed without a real {@link io.vertx.redis.client.RedisAPI};
 * we call the package-private {@code computeDiff(Snapshot, Map)} method directly so
 * tests are deterministic and fast (no I/O).
 *
 * <h3>Golden cases per spec</h3>
 * <ol>
 *   <li>Same itemId, same version → NOOP</li>
 *   <li>Same itemId, higher version → UPSERT</li>
 *   <li>Different itemId (itemId switched) → DELETE old + UPSERT new</li>
 *   <li>No prior cache (first snapshot) → UPSERT</li>
 *   <li>newVersion &lt; storedVersion → VERSION_ANOMALY (metric, no write)</li>
 * </ol>
 */
class ComputersDiffEngineTest {

    private static final String TOPIC       = "computers";
    private static final String PRODUCER_ID = "prod-test";

    /** Engine under test — null RedisAPI is fine because computeDiff() is pure. */
    private ComputersDiffEngine engine;

    @BeforeEach
    void setUp() {
        engine = new ComputersDiffEngine(null, "bp");
    }

    // ---- Golden case 1: same itemId, same version --------------------------------

    @Test
    @DisplayName("Same itemId and version → NOOP plan")
    void same_item_same_version_is_noop() {
        Map<String, Long> cache    = Map.of("item-1", 5L);
        Snapshot          snapshot = snap("item-1", 5L);

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.isEmpty()).isTrue();
        assertThat(plan.upserts()).isEmpty();
        assertThat(plan.deletes()).isEmpty();
    }

    // ---- Golden case 2: same itemId, higher version ------------------------------

    @Test
    @DisplayName("Same itemId with higher version → single UPSERT")
    void same_item_higher_version_is_upsert() {
        Map<String, Long> cache    = Map.of("item-1", 5L);
        Snapshot          snapshot = snap("item-1", 6L);

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.upserts()).hasSize(1);
        assertThat(plan.upserts().get(0).itemId()).isEqualTo("item-1");
        assertThat(plan.upserts().get(0).version()).isEqualTo(6L);
        assertThat(plan.deletes()).isEmpty();
    }

    // ---- Golden case 3: itemId switched -----------------------------------------

    @Test
    @DisplayName("Different itemId → DELETE old + UPSERT new")
    void switched_item_id_deletes_old_and_upserts_new() {
        Map<String, Long> cache    = Map.of("item-old", 3L);
        Snapshot          snapshot = snap("item-new", 1L);

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.upserts()).hasSize(1);
        assertThat(plan.upserts().get(0).itemId()).isEqualTo("item-new");

        assertThat(plan.deletes()).hasSize(1);
        assertThat(plan.deletes().get(0)).isEqualTo("item-old");
    }

    // ---- Golden case 4: no prior cache (first snapshot) -------------------------

    @Test
    @DisplayName("Empty cache (first snapshot for this producer) → single UPSERT, no DELETE")
    void first_snapshot_with_empty_cache_is_upsert() {
        Map<String, Long> emptyCache = Map.of();
        Snapshot          snapshot   = snap("item-1", 1L);

        WritePlan plan = engine.computeDiff(snapshot, emptyCache);

        assertThat(plan.upserts()).hasSize(1);
        assertThat(plan.upserts().get(0).itemId()).isEqualTo("item-1");
        assertThat(plan.deletes()).isEmpty();
    }

    // ---- Negative case: version anomaly -----------------------------------------

    @Test
    @DisplayName("newVersion < storedVersion → anomaly: empty plan (item skipped)")
    void lower_version_is_version_anomaly_and_produces_noop() {
        Map<String, Long> cache    = Map.of("item-1", 10L);
        Snapshot          snapshot = snap("item-1", 9L);

        WritePlan plan = engine.computeDiff(snapshot, cache);

        // Anomalous item must NOT be applied
        assertThat(plan.upserts()).isEmpty();
        assertThat(plan.deletes()).isEmpty();
        assertThat(plan.isEmpty()).isTrue();
    }

    // ---- Edge case: empty snapshot (producer sent no items) ---------------------

    @Test
    @DisplayName("Empty snapshot (no items) with existing cache → all cached items deleted")
    void empty_snapshot_deletes_all_cached_items() {
        Map<String, Long> cache    = Map.of("item-1", 5L);
        Snapshot          snapshot = snapEmpty();

        WritePlan plan = engine.computeDiff(snapshot, cache);

        assertThat(plan.upserts()).isEmpty();
        assertThat(plan.deletes()).containsExactly("item-1");
    }

    // ---- Helpers ----------------------------------------------------------------

    private static Snapshot snap(String itemId, long version) {
        return Snapshot.fromJson(new JsonObject()
                .put("topic",      TOPIC)
                .put("producerId", PRODUCER_ID)
                .put("msgTs",      1000L)
                .put("items",      new JsonArray()
                        .add(new JsonObject()
                                .put("itemId",   itemId)
                                .put("version",  version)
                                .put("dataJson", "{\"name\":\"test\"}"))));
    }

    private static Snapshot snapEmpty() {
        return Snapshot.fromJson(new JsonObject()
                .put("topic",      TOPIC)
                .put("producerId", PRODUCER_ID)
                .put("msgTs",      1000L)
                .put("items",      new JsonArray()));
    }
}
