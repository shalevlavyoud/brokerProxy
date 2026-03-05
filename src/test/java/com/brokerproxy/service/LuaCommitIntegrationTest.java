package com.brokerproxy.service;

import com.brokerproxy.model.CommitResult;
import com.brokerproxy.model.CommitResult.CommitStatus;
import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.SnapshotItem;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link LuaCommitScript} against a real Redis instance
 * (Testcontainers — requires Docker).
 *
 * <h3>Invariants verified</h3>
 * <ol>
 *   <li>Happy path: epoch matches, msgTs new → OK, state + indices written</li>
 *   <li>Fencing: wrong epoch → FENCED, nothing written</li>
 *   <li>Recency drop: msgTs ≤ lastAccepted → DROPPED_RECENCY, nothing written</li>
 *   <li>SEQ allocation: multiple upserts receive sequential SEQs</li>
 *   <li>Deletes: HDEL from state + ZADD to ch:del + HDEL from prodver</li>
 *   <li>Atomicity: state hashes and ZSETs are unchanged after FENCED / DROPPED_RECENCY</li>
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LuaCommitIntegrationTest {

    private static final String PREFIX = "bp";
    private static final long   EPOCH  = 1L;

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    static { REDIS.start(); }

    private Vertx           vertx;
    private RedisAPI        api;
    private LuaCommitScript script;

    @BeforeAll
    void setupAll() throws Exception {
        vertx = Vertx.vertx();
        String connStr = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        Redis client = Redis.createClient(vertx, new RedisOptions().setConnectionString(connStr));
        api    = RedisAPI.api(client);
        script = new LuaCommitScript(api, PREFIX);
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

    // ---- Happy path -------------------------------------------------------------

    @Test
    @DisplayName("Valid epoch + new msgTs → OK; state, index, recency, prodver written")
    void happy_path_writes_all_keys() throws Exception {
        seedEpoch(EPOCH);

        WritePlan plan = planWithUpserts("computers", "prod-1", 100L,
                List.of(new SnapshotItem("item-A", 5L, "{\"x\":1}")));

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isOk()).isTrue();
        assertThat(result.appliedUpserts()).isEqualTo(1);
        assertThat(result.appliedDeletes()).isEqualTo(0);
        assertThat(result.firstSeq()).isEqualTo(1L);
        assertThat(result.lastSeq()).isEqualTo(1L);

        // State written
        assertHsetField("bp:state:v:computers", "item-A", "5");
        assertHsetField("bp:state:j:computers", "item-A", "{\"x\":1}");

        // Change index: item-A at seq=1 in upserts zset
        assertZscore("bp:ch:up:computers", "item-A", 1.0);

        // Producer version cache updated (scalar key for computers)
        assertStringKey("bp:compver:computers:prod-1", "item-A|5");

        // Recency updated
        assertStringKey("bp:recency:computers:prod-1", "100");
    }

    // ---- Fencing ----------------------------------------------------------------

    @Test
    @DisplayName("Wrong epoch → FENCED; no state written")
    void wrong_epoch_returns_fenced_and_writes_nothing() throws Exception {
        seedEpoch(EPOCH);  // Redis has epoch=1

        WritePlan plan = planWithUpserts("computers", "prod-2", 200L,
                List.of(new SnapshotItem("item-B", 3L, "{}")));

        CommitResult result = await(script.eval(plan, EPOCH + 1L));  // wrong epoch

        assertThat(result.isFenced()).isTrue();
        assertThat(result.firstSeq()).isEqualTo(-1L);

        // Nothing should have been written
        assertKeyAbsent("bp:state:v:computers");
        assertKeyAbsent("bp:seq:computers");
    }

    @Test
    @DisplayName("Epoch absent in Redis (treated as 0) → FENCED for epoch=1")
    void absent_epoch_key_fences_non_zero_caller() throws Exception {
        // bp:leader:epoch NOT seeded → Lua treats stored as "0"
        WritePlan plan = planWithUpserts("computers", "prod-3", 300L,
                List.of(new SnapshotItem("item-C", 1L, "{}")));

        CommitResult result = await(script.eval(plan, EPOCH));  // expects 1, stored=0

        assertThat(result.isFenced()).isTrue();
        assertKeyAbsent("bp:state:v:computers");
    }

    // ---- Recency ----------------------------------------------------------------

    @Test
    @DisplayName("msgTs <= lastAccepted → DROPPED_RECENCY; state hashes untouched")
    void stale_msgts_returns_dropped_recency_and_writes_nothing() throws Exception {
        seedEpoch(EPOCH);
        seedRecency("computers", "prod-4", 500L);  // last accepted = 500

        WritePlan plan = planWithUpserts("computers", "prod-4", 499L,  // out-of-order
                List.of(new SnapshotItem("item-D", 2L, "{}")));

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isRecencyDrop()).isTrue();
        assertKeyAbsent("bp:state:v:computers");
        assertKeyAbsent("bp:seq:computers");

        // Recency key must still be the old value (not overwritten)
        assertStringKey("bp:recency:computers:prod-4", "500");
    }

    @Test
    @DisplayName("Duplicate msgTs (== lastAccepted) → DROPPED_RECENCY")
    void duplicate_msgts_is_dropped_recency() throws Exception {
        seedEpoch(EPOCH);
        seedRecency("computers", "prod-5", 300L);

        WritePlan plan = planWithUpserts("computers", "prod-5", 300L,  // duplicate
                List.of(new SnapshotItem("item-E", 1L, "{}")));

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isRecencyDrop()).isTrue();
    }

    // ---- SEQ allocation ---------------------------------------------------------

    @Test
    @DisplayName("Two upserts in one plan receive sequential SEQs starting at 1")
    void two_upserts_get_sequential_seqs() throws Exception {
        seedEpoch(EPOCH);

        WritePlan plan = planWithUpserts("headsets", "prod-h", 100L, List.of(
                new SnapshotItem("h-1", 1L, "{}"),
                new SnapshotItem("h-2", 1L, "{}")));

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isOk()).isTrue();
        assertThat(result.firstSeq()).isEqualTo(1L);
        assertThat(result.lastSeq()).isEqualTo(2L);
        assertThat(result.appliedUpserts()).isEqualTo(2);

        // h-1 at seq=1, h-2 at seq=2
        assertZscore("bp:ch:up:headsets", "h-1", 1.0);
        assertZscore("bp:ch:up:headsets", "h-2", 2.0);

        // Headsets uses Hash prodver (isScalar=0 path)
        assertHsetField("bp:prodver:headsets:prod-h", "h-1", "1");
        assertHsetField("bp:prodver:headsets:prod-h", "h-2", "1");
    }

    @Test
    @DisplayName("Sequential commits accumulate SEQ correctly")
    void sequential_commits_increment_seq() throws Exception {
        seedEpoch(EPOCH);

        WritePlan plan1 = planWithUpserts("computers", "prod-6", 100L,
                List.of(new SnapshotItem("c-1", 1L, "{}")));
        WritePlan plan2 = planWithUpserts("computers", "prod-6", 200L,
                List.of(new SnapshotItem("c-1", 2L, "{}")));

        CommitResult r1 = await(script.eval(plan1, EPOCH));
        CommitResult r2 = await(script.eval(plan2, EPOCH));

        assertThat(r1.firstSeq()).isEqualTo(1L);
        assertThat(r1.lastSeq()).isEqualTo(1L);
        assertThat(r2.firstSeq()).isEqualTo(2L);
        assertThat(r2.lastSeq()).isEqualTo(2L);
    }

    // ---- Deletes ----------------------------------------------------------------

    @Test
    @DisplayName("Delete removes from state + prodver, adds to ch:del at next SEQ")
    void delete_removes_state_and_updates_del_index() throws Exception {
        seedEpoch(EPOCH);

        // First, seed state as if a previous commit put item-X there
        await(api.hset(List.of("bp:state:v:computers", "item-X", "3")));
        await(api.hset(List.of("bp:state:j:computers", "item-X", "{\"old\":true}")));
        // Computers uses scalar key: "bp:compver:computers:{producerId}" = "itemId|version"
        await(api.set(List.of("bp:compver:computers:prod-7", "item-X|3")));

        WritePlan plan = new WritePlan("computers", "prod-7", 400L,
                List.of(), List.of("item-X"));

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isOk()).isTrue();
        assertThat(result.appliedDeletes()).isEqualTo(1);
        assertThat(result.appliedUpserts()).isEqualTo(0);

        // item-X must be gone from state
        var stateV = await(api.hget("bp:state:v:computers", "item-X"));
        assertThat(stateV).isNull();
        // Scalar compver key must have been DELeted (pure delete, no upsert)
        assertKeyAbsent("bp:compver:computers:prod-7");

        // item-X must appear in the deletes change-index
        assertZscore("bp:ch:del:computers", "item-X", 1.0);
    }

    // ---- NOOP plan (no changes) -------------------------------------------------

    @Test
    @DisplayName("NOOP plan (no upserts, no deletes) → OK, recency updated, seq unchanged")
    void noop_plan_is_ok_and_updates_recency_only() throws Exception {
        seedEpoch(EPOCH);

        WritePlan plan = WritePlan.noop("computers", "prod-8", 500L);

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isOk()).isTrue();
        assertThat(result.firstSeq()).isEqualTo(-1L);  // no seq allocated
        assertThat(result.lastSeq()).isEqualTo(-1L);
        assertThat(result.appliedUpserts()).isEqualTo(0);
        assertThat(result.appliedDeletes()).isEqualTo(0);

        // Recency still updated so msgTs advances even on NOOP
        assertStringKey("bp:recency:computers:prod-8", "500");

        // SEQ counter must not have been incremented
        assertKeyAbsent("bp:seq:computers");
    }

    // ---- Helpers ----------------------------------------------------------------

    private void seedEpoch(long epoch) throws Exception {
        await(api.set(List.of(PREFIX + ":leader:epoch", String.valueOf(epoch))));
    }

    private void seedRecency(String topic, String producerId, long msgTs) throws Exception {
        await(api.set(List.of(PREFIX + ":recency:" + topic + ":" + producerId,
                String.valueOf(msgTs))));
    }

    private static WritePlan planWithUpserts(String topic, String producerId, long msgTs,
                                              List<SnapshotItem> upserts) {
        return new WritePlan(topic, producerId, msgTs, upserts, List.of());
    }

    private void assertHsetField(String key, String field, String expected) throws Exception {
        var val = await(api.hget(key, field));
        assertThat(val).as("HGET %s %s", key, field).isNotNull();
        assertThat(val.toString()).isEqualTo(expected);
    }

    private void assertStringKey(String key, String expected) throws Exception {
        var val = await(api.get(key));
        assertThat(val).as("GET %s", key).isNotNull();
        assertThat(val.toString()).isEqualTo(expected);
    }

    private void assertKeyAbsent(String key) throws Exception {
        var exists = await(api.exists(List.of(key)));
        assertThat(exists.toLong()).as("EXISTS %s should be 0", key).isEqualTo(0L);
    }

    private void assertZscore(String key, String member, double expectedScore) throws Exception {
        var score = await(api.zscore(key, member));
        assertThat(score).as("ZSCORE %s %s", key, member).isNotNull();
        assertThat(Double.parseDouble(score.toString())).isEqualTo(expectedScore);
    }

    private <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
}
