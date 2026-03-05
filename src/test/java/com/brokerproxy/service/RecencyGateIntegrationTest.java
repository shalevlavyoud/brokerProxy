package com.brokerproxy.service;

import com.brokerproxy.model.RecencyResult;
import com.brokerproxy.model.Snapshot;
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
 * Integration tests for {@link RecencyGate} against a real Redis instance
 * (Testcontainers — requires Docker).
 *
 * <p>Tests the key invariant: only snapshots with {@code msgTs > lastAccepted}
 * pass the gate, including the canonical sequence {@code 10 → 9 → 10} where
 * only the first snapshot is accepted.
 *
 * <p>Note: recency advancement is now performed exclusively inside the atomic
 * Lua commit script (BE-06).  These tests simulate that by writing directly to
 * Redis via the API — {@link RecencyGate} itself is read-only.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RecencyGateIntegrationTest {

    // ---- Container setup --------------------------------------------------------

    @SuppressWarnings("resource")
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
                    .withExposedPorts(6379);

    static { REDIS.start(); }

    // ---- Fixtures ---------------------------------------------------------------

    private Vertx       vertx;
    private RedisAPI    api;
    private RecencyGate gate;

    @BeforeAll
    void setupAll() throws Exception {
        vertx = Vertx.vertx();

        String connStr = "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
        Redis client = Redis.createClient(vertx,
                new RedisOptions().setConnectionString(connStr));
        api  = RedisAPI.api(client);
        gate = new RecencyGate(api, "bp");
    }

    @BeforeEach
    void flushRedis() throws Exception {
        // Clean slate for each test — safe because this is a dedicated test container
        await(api.flushall(List.of()));
    }

    @AfterAll
    void tearDownAll() throws Exception {
        REDIS.stop();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    // ---- Check tests ------------------------------------------------------------

    @Test
    @DisplayName("First snapshot (no recency key) is accepted")
    void first_snapshot_accepted_when_no_key() throws Exception {
        RecencyResult result = await(gate.check(snap("computers", "prod-1", 100L)));

        assertThat(result.wasAccepted()).isTrue();
        assertThat(result.lastAcceptedTs()).isEqualTo(0L);
    }

    @Test
    @DisplayName("Snapshot with msgTs > stored value is accepted")
    void newer_msgTs_is_accepted() throws Exception {
        seedRecency("computers", "prod-2", 100L);

        RecencyResult result = await(gate.check(snap("computers", "prod-2", 101L)));

        assertThat(result.wasAccepted()).isTrue();
    }

    @Test
    @DisplayName("Snapshot with msgTs == stored value is dropped (strict ordering)")
    void duplicate_msgTs_is_dropped() throws Exception {
        seedRecency("computers", "prod-3", 100L);

        RecencyResult result = await(gate.check(snap("computers", "prod-3", 100L)));

        assertThat(result.wasAccepted()).isFalse();
        assertThat(result.lastAcceptedTs()).isEqualTo(100L);
    }

    @Test
    @DisplayName("Snapshot with msgTs < stored value is dropped (out-of-order)")
    void older_msgTs_is_dropped() throws Exception {
        seedRecency("computers", "prod-4", 100L);

        RecencyResult result = await(gate.check(snap("computers", "prod-4", 99L)));

        assertThat(result.wasAccepted()).isFalse();
        assertThat(result.lastAcceptedTs()).isEqualTo(100L);
    }

    @Test
    @DisplayName("Recency keys are isolated per (topic, producerId)")
    void recency_isolated_per_topic_and_producer() throws Exception {
        seedRecency("computers", "prod-A", 500L);

        // Different topic — independent gate
        RecencyResult r1 = await(gate.check(snap("headsets", "prod-A", 1L)));
        assertThat(r1.wasAccepted()).isTrue();

        // Same topic, different producer — independent gate
        RecencyResult r2 = await(gate.check(snap("computers", "prod-B", 1L)));
        assertThat(r2.wasAccepted()).isTrue();
    }

    // ---- Canonical sequence test ------------------------------------------------

    @Test
    @DisplayName("Sequence msgTs=10 → 9 → 10: only the first snapshot is accepted (spec invariant)")
    void sequence_10_9_10_only_first_accepted() throws Exception {
        String topic      = "computers";
        String producerId = "prod-seq";

        // 1. msgTs=10 — no recency key → ACCEPTED
        RecencyResult r1 = await(gate.check(snap(topic, producerId, 10L)));
        assertThat(r1.wasAccepted()).as("msgTs=10 (first) should be accepted").isTrue();

        // Simulate what the Lua commit script does atomically: advance recency to 10
        await(api.set(List.of("bp:recency:computers:prod-seq", "10")));

        // 2. msgTs=9 — 9 <= 10 → DROPPED
        RecencyResult r2 = await(gate.check(snap(topic, producerId, 9L)));
        assertThat(r2.wasAccepted()).as("msgTs=9 (out-of-order) should be dropped").isFalse();
        assertThat(r2.lastAcceptedTs()).isEqualTo(10L);

        // 3. msgTs=10 — 10 <= 10 → DROPPED (duplicate)
        RecencyResult r3 = await(gate.check(snap(topic, producerId, 10L)));
        assertThat(r3.wasAccepted()).as("msgTs=10 (duplicate) should be dropped").isFalse();
        assertThat(r3.lastAcceptedTs()).isEqualTo(10L);
    }

    // ---- Helpers ----------------------------------------------------------------

    private static Snapshot snap(String topic, String producerId, long msgTs) {
        return Snapshot.fromJson(new JsonObject()
                .put("topic", topic)
                .put("producerId", producerId)
                .put("msgTs", msgTs)
                .put("items", new JsonArray()));
    }

    private void seedRecency(String topic, String producerId, long msgTs) throws Exception {
        String key = "bp:recency:" + topic + ":" + producerId;
        await(api.set(List.of(key, String.valueOf(msgTs))));
    }

    private <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
}
