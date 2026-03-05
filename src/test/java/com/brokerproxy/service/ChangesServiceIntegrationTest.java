package com.brokerproxy.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link ChangesService} against a real Redis instance
 * (Testcontainers -- requires Docker).
 *
 * <h3>Invariants verified</h3>
 * <ol>
 *   <li>Empty index -- sinceSeq=0, no data: newSeq=0, empty lists</li>
 *   <li>Upserts only -- 3 entries: all returned, newSeq=max</li>
 *   <li>Deletes only -- 2 entries: in deleted[], newSeq=max</li>
 *   <li>Mixed -- upserts + deletes without overlap: both lists populated</li>
 *   <li>Conflict: upsert wins -- same id in up(seq=8) and del(seq=5) -> in items[]</li>
 *   <li>Conflict: delete wins -- same id in up(seq=3) and del(seq=9) -> in deleted[]</li>
 *   <li>Pagination (BE-09) -- 25 entries, 3 pages of limit=10 cover all without gaps</li>
 *   <li>fullResyncRequired -- sinceSeq below bp:seq:min:{topic}</li>
 *   <li>State miss -- itemId in up-index but absent from state hashes -> skipped</li>
 *   <li>Limit respected -- seed 200 entries, limit=50 -> exactly 50 returned</li>
 * </ol>
 *
 * <p>All tests use {@code @TestInstance(PER_CLASS)} + Testcontainers pattern,
 * matching the existing {@code RecencyGateIntegrationTest} and
 * {@code LuaCommitIntegrationTest} conventions.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChangesServiceIntegrationTest {

    private static final String PREFIX = "bp";
    private static final String TOPIC  = "computers";

    // ---- Redis key helpers -------------------------------------------------------

    private static String upKey()     { return PREFIX + ":ch:up:"   + TOPIC; }
    private static String delKey()    { return PREFIX + ":ch:del:"  + TOPIC; }
    private static String stateVKey() { return PREFIX + ":state:v:" + TOPIC; }
    private static String stateJKey() { return PREFIX + ":state:j:" + TOPIC; }
    private static String minKey()    { return PREFIX + ":seq:min:" + TOPIC; }

    // ---- Container setup --------------------------------------------------------

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    static { REDIS.start(); }

    // ---- Fixtures ---------------------------------------------------------------

    private Vertx          vertx;
    private RedisAPI       api;
    private ChangesService service;

    @BeforeAll
    void setupAll() throws Exception {
        vertx = Vertx.vertx();

        String connStr = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        Redis client = Redis.createClient(vertx, new RedisOptions().setConnectionString(connStr));
        api     = RedisAPI.api(client);
        service = new ChangesService(api, PREFIX);
    }

    @BeforeEach
    void flushRedis() throws Exception {
        await(api.flushall(List.of()));
    }

    @AfterAll
    void tearDownAll() throws Exception {
        REDIS.stop();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    // ---- Test 1: empty index ----------------------------------------------------

    @Test
    @DisplayName("Empty index: sinceSeq=0, no data -> newSeq=0, empty lists")
    void empty_index_returns_empty_response() throws Exception {
        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 10));

        assertThat(resp.fullResyncRequired()).isFalse();
        assertThat(resp.items()).isEmpty();
        assertThat(resp.deleted()).isEmpty();
        assertThat(resp.newSeq()).isEqualTo(0L);
    }

    // ---- Test 2: upserts only ---------------------------------------------------

    @Test
    @DisplayName("Upserts only: 3 entries at seq 1,2,3 -> all returned, newSeq=3")
    void upserts_only_returned_correctly() throws Exception {
        seedUpsertState("item-1", 1L, 1L, "{\"a\":1}");
        seedUpsertState("item-2", 2L, 2L, "{\"a\":2}");
        seedUpsertState("item-3", 3L, 3L, "{\"a\":3}");

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 10));

        assertThat(resp.fullResyncRequired()).isFalse();
        assertThat(resp.items()).hasSize(3);
        assertThat(resp.deleted()).isEmpty();
        assertThat(resp.newSeq()).isEqualTo(3L);

        Set<String> ids = new HashSet<>();
        for (ChangesService.ChangesItem item : resp.items()) {
            ids.add(item.itemId());
            assertThat(item.version()).isGreaterThanOrEqualTo(1L);
        }
        assertThat(ids).containsExactlyInAnyOrder("item-1", "item-2", "item-3");
    }

    // ---- Test 3: deletes only ---------------------------------------------------

    @Test
    @DisplayName("Deletes only: 2 entries -> in deleted[], newSeq=max")
    void deletes_only_returned_in_deleted_list() throws Exception {
        seedDelete("item-del-1", 1L);
        seedDelete("item-del-2", 2L);

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 10));

        assertThat(resp.fullResyncRequired()).isFalse();
        assertThat(resp.items()).isEmpty();
        assertThat(resp.deleted()).containsExactlyInAnyOrder("item-del-1", "item-del-2");
        assertThat(resp.newSeq()).isEqualTo(2L);
    }

    // ---- Test 4: mixed upserts and deletes (no overlap) -------------------------

    @Test
    @DisplayName("Mixed: upserts + deletes without overlap -> both lists populated")
    void mixed_upserts_and_deletes() throws Exception {
        seedUpsertState("item-up-1", 1L, 1L, "{}");
        seedUpsertState("item-up-2", 2L, 2L, "{}");
        seedDelete("item-del-1", 3L);
        seedDelete("item-del-2", 4L);

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 10));

        assertThat(resp.items()).hasSize(2);
        assertThat(resp.deleted()).hasSize(2);
        assertThat(resp.newSeq()).isEqualTo(4L);

        Set<String> itemIds = new HashSet<>();
        for (ChangesService.ChangesItem item : resp.items()) {
            itemIds.add(item.itemId());
        }
        assertThat(itemIds).containsExactlyInAnyOrder("item-up-1", "item-up-2");
        assertThat(resp.deleted()).containsExactlyInAnyOrder("item-del-1", "item-del-2");
    }

    // ---- Test 5: conflict - upsert wins -----------------------------------------

    @Test
    @DisplayName("Conflict: upsert wins -- up(seq=8) beats del(seq=5) -> in items[]")
    void conflict_upsert_wins() throws Exception {
        // item-conflict appears in BOTH up(seq=8) and del(seq=5)
        await(api.zadd(List.of(upKey(),  "8", "item-conflict")));
        await(api.zadd(List.of(delKey(), "5", "item-conflict")));

        // State is present since the upsert is the winner
        await(api.hset(List.of(stateVKey(), "item-conflict", "8")));
        await(api.hset(List.of(stateJKey(), "item-conflict", "{\"winner\":\"up\"}")));

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 10));

        assertThat(resp.deleted()).doesNotContain("item-conflict");
        assertThat(resp.items()).hasSize(1);
        assertThat(resp.items().get(0).itemId()).isEqualTo("item-conflict");
        assertThat(resp.newSeq()).isEqualTo(8L);
    }

    // ---- Test 6: conflict - delete wins -----------------------------------------

    @Test
    @DisplayName("Conflict: delete wins -- del(seq=9) beats up(seq=3) -> in deleted[]")
    void conflict_delete_wins() throws Exception {
        // item-conflict appears in BOTH del(seq=9) and up(seq=3)
        await(api.zadd(List.of(delKey(), "9", "item-conflict")));
        await(api.zadd(List.of(upKey(),  "3", "item-conflict")));

        // No state seeding needed -- item should appear only in deleted[]

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 10));

        assertThat(resp.items()).isEmpty();
        assertThat(resp.deleted()).containsExactly("item-conflict");
        assertThat(resp.newSeq()).isEqualTo(9L);
    }

    // ---- Test 7: pagination (BE-09) ---------------------------------------------

    @Test
    @DisplayName("Pagination (BE-09): 25 upserts, 3 calls with limit=10 cover all without gaps")
    void pagination_covers_all_entries() throws Exception {
        // Seed 25 upserts at seq 1..25
        for (int i = 1; i <= 25; i++) {
            String itemId = "item-" + i;
            await(api.zadd(List.of(upKey(), String.valueOf(i), itemId)));
            await(api.hset(List.of(stateVKey(), itemId, String.valueOf(i))));
            await(api.hset(List.of(stateJKey(), itemId, "{}")));
        }

        Set<String> allReturned = new HashSet<>();
        long sinceSeq = 0L;

        // Page 1: sinceSeq=0, limit=10 -> items 1..10, newSeq=10
        ChangesService.ChangesResponse page1 = await(service.query(TOPIC, sinceSeq, 10));
        assertThat(page1.items()).hasSize(10);
        for (ChangesService.ChangesItem it : page1.items()) allReturned.add(it.itemId());
        sinceSeq = page1.newSeq();
        assertThat(sinceSeq).isEqualTo(10L);

        // Page 2: sinceSeq=10, limit=10 -> items 11..20, newSeq=20
        ChangesService.ChangesResponse page2 = await(service.query(TOPIC, sinceSeq, 10));
        assertThat(page2.items()).hasSize(10);
        for (ChangesService.ChangesItem it : page2.items()) allReturned.add(it.itemId());
        sinceSeq = page2.newSeq();
        assertThat(sinceSeq).isEqualTo(20L);

        // Page 3: sinceSeq=20, limit=10 -> items 21..25, newSeq=25
        ChangesService.ChangesResponse page3 = await(service.query(TOPIC, sinceSeq, 10));
        assertThat(page3.items()).hasSize(5);
        for (ChangesService.ChangesItem it : page3.items()) allReturned.add(it.itemId());
        sinceSeq = page3.newSeq();
        assertThat(sinceSeq).isEqualTo(25L);

        // All 25 distinct items returned across the 3 pages
        assertThat(allReturned).hasSize(25);

        // Page 4: no more entries
        ChangesService.ChangesResponse page4 = await(service.query(TOPIC, sinceSeq, 10));
        assertThat(page4.items()).isEmpty();
        assertThat(page4.deleted()).isEmpty();
        assertThat(page4.newSeq()).isEqualTo(25L);  // stays at sinceSeq when empty
    }

    // ---- Test 8: fullResyncRequired ---------------------------------------------

    @Test
    @DisplayName("fullResyncRequired: sinceSeq=30 below bp:seq:min=50 -> flag=true, empty lists")
    void fullResync_when_sinceSeq_below_min() throws Exception {
        await(api.set(List.of(minKey(), "50")));

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 30L, 10));

        assertThat(resp.fullResyncRequired()).isTrue();
        assertThat(resp.items()).isEmpty();
        assertThat(resp.deleted()).isEmpty();
        assertThat(resp.newSeq()).isEqualTo(30L);  // newSeq = sinceSeq when fullResync
    }

    @Test
    @DisplayName("sinceSeq exactly at bp:seq:min is NOT a fullResync (boundary)")
    void sinceSeq_at_min_is_not_full_resync() throws Exception {
        await(api.set(List.of(minKey(), "50")));

        // sinceSeq == minRetained -> not < minRetained -> proceed normally
        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 50L, 10));

        assertThat(resp.fullResyncRequired()).isFalse();
    }

    // ---- Test 9: state miss -----------------------------------------------------

    @Test
    @DisplayName("State miss: itemId in up-index but not in state hashes -> skipped from items")
    void state_miss_skipped_from_items() throws Exception {
        // Orphan item is in the change index but NOT in state hashes
        await(api.zadd(List.of(upKey(), "1", "orphan-item")));

        // A normal item that has state
        seedUpsertState("normal-item", 2L, 2L, "{}");

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 10));

        assertThat(resp.items()).hasSize(1);
        assertThat(resp.items().get(0).itemId()).isEqualTo("normal-item");
        // orphan-item is NOT in items (state miss)
        List<String> returnedIds = new ArrayList<>();
        for (ChangesService.ChangesItem it : resp.items()) returnedIds.add(it.itemId());
        assertThat(returnedIds).doesNotContain("orphan-item");
    }

    // ---- Test 10: limit respected -----------------------------------------------

    @Test
    @DisplayName("Limit=50 with 200 seeded entries returns exactly 50 items")
    void limit_caps_result_count() throws Exception {
        for (int i = 1; i <= 200; i++) {
            String itemId = "item-" + i;
            await(api.zadd(List.of(upKey(), String.valueOf(i), itemId)));
            await(api.hset(List.of(stateVKey(), itemId, String.valueOf(i))));
            await(api.hset(List.of(stateJKey(), itemId, "{}")));
        }

        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 0L, 50));

        assertThat(resp.items()).hasSize(50);
        assertThat(resp.newSeq()).isEqualTo(50L);  // max seq of the 50 returned
    }

    // ---- Test 11: sinceSeq filters correctly ------------------------------------

    @Test
    @DisplayName("sinceSeq filters out older entries (exclusive lower bound)")
    void sinceSeq_filters_older_entries() throws Exception {
        seedUpsertState("old-item", 1L, 5L, "{}");
        seedUpsertState("new-item", 6L, 6L, "{}");

        // sinceSeq=5 -> only entries with seq > 5 returned (exclusive)
        ChangesService.ChangesResponse resp = await(service.query(TOPIC, 5L, 10));

        assertThat(resp.items()).hasSize(1);
        assertThat(resp.items().get(0).itemId()).isEqualTo("new-item");
        assertThat(resp.newSeq()).isEqualTo(6L);
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Seeds a single upsert: ZADD to the up-index, HSET to state:v and state:j.
     *
     * @param itemId   item identifier
     * @param seq      seq score in the change index
     * @param version  version stored in state:v
     * @param dataJson payload stored in state:j
     */
    private void seedUpsertState(String itemId, long seq, long version, String dataJson)
            throws Exception {
        await(api.zadd(List.of(upKey(), String.valueOf(seq), itemId)));
        await(api.hset(List.of(stateVKey(), itemId, String.valueOf(version))));
        await(api.hset(List.of(stateJKey(), itemId, dataJson)));
    }

    /** Seeds a single delete: ZADD to the del-index only. */
    private void seedDelete(String itemId, long seq) throws Exception {
        await(api.zadd(List.of(delKey(), String.valueOf(seq), itemId)));
    }

    private <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
}
