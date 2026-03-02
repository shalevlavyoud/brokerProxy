package com.brokerproxy.service;

import com.brokerproxy.model.CommitResult;
import com.brokerproxy.model.SnapshotItem;
import com.brokerproxy.model.WritePlan;
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

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link LuaCommitScript} EVALSHA lifecycle.
 *
 * <p>Verifies:
 * <ol>
 *   <li>Script loads successfully ({@code SCRIPT LOAD}) and EVALSHA commit works.</li>
 *   <li>After {@code SCRIPT FLUSH}, the next commit detects NOSCRIPT, reloads the
 *       SHA automatically, and succeeds — no caller intervention required.</li>
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LuaCommitEvalshaIntegrationTest {

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

        // Pre-load the script once at "startup"
        await(script.loadScript());
    }

    @BeforeEach
    void flushRedis() throws Exception {
        await(api.flushall(List.of()));
        // Re-seed epoch after flush
        await(api.set(List.of(PREFIX + ":leader:epoch", String.valueOf(EPOCH))));
    }

    @AfterAll
    void tearDownAll() throws Exception {
        REDIS.stop();
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    // ---- EVALSHA happy path -----------------------------------------------------

    @Test
    @DisplayName("EVALSHA commit succeeds after script pre-loaded")
    void evalsha_commit_succeeds_after_load() throws Exception {
        WritePlan plan = plan("computers", "prod-1", 100L,
                new SnapshotItem("item-A", 1L, "{}"));

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isOk()).isTrue();
        assertThat(result.appliedUpserts()).isEqualTo(1);

        // Confirm the SHA-based path was used: state should be written
        var stateV = await(api.hget("bp:state:v:computers", "item-A"));
        assertThat(stateV).isNotNull();
        assertThat(stateV.toString()).isEqualTo("1");
    }

    // ---- NOSCRIPT auto-reload ---------------------------------------------------

    @Test
    @DisplayName("SCRIPT FLUSH → NOSCRIPT → auto-reload SHA → commit succeeds")
    void noscript_triggers_reload_and_commit_succeeds() throws Exception {
        // Flush the Redis script cache — this evicts our script's SHA
        await(api.script(List.of("FLUSH")));

        // The next eval() will get NOSCRIPT from Redis,
        // auto-reload the script, and retry EVALSHA transparently.
        WritePlan plan = plan("computers", "prod-2", 200L,
                new SnapshotItem("item-B", 2L, "{}"));

        CommitResult result = await(script.eval(plan, EPOCH));

        assertThat(result.isOk())
                .as("Commit should succeed after transparent NOSCRIPT reload")
                .isTrue();
        assertThat(result.appliedUpserts()).isEqualTo(1);

        // Verify state was actually written
        var stateV = await(api.hget("bp:state:v:computers", "item-B"));
        assertThat(stateV).isNotNull();
        assertThat(stateV.toString()).isEqualTo("2");
    }

    @Test
    @DisplayName("Multiple commits after SCRIPT FLUSH are all stable")
    void multiple_commits_after_noscript_are_stable() throws Exception {
        await(api.script(List.of("FLUSH")));

        CommitResult r1 = await(script.eval(
                plan("computers", "prod-3", 100L, new SnapshotItem("c-1", 1L, "{}")), EPOCH));
        CommitResult r2 = await(script.eval(
                plan("computers", "prod-3", 200L, new SnapshotItem("c-1", 2L, "{}")), EPOCH));

        assertThat(r1.isOk()).isTrue();
        assertThat(r2.isOk()).isTrue();
        assertThat(r1.lastSeq()).isEqualTo(1L);
        assertThat(r2.lastSeq()).isEqualTo(2L);
    }

    // ---- Helpers ----------------------------------------------------------------

    private static WritePlan plan(String topic, String producerId, long msgTs,
                                   SnapshotItem... items) {
        return new WritePlan(topic, producerId, msgTs, List.of(items), List.of());
    }

    private <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
}
