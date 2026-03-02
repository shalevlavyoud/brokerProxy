package com.brokerproxy.service;

import com.brokerproxy.model.CommitResult;
import com.brokerproxy.model.CommitResult.CommitStatus;
import com.brokerproxy.model.SnapshotItem;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link CommitExecutor} retry and mapping behaviour.
 *
 * <p>Uses Mockito to mock {@link LuaCommitScript} so the tests run without a
 * real Redis instance and focus purely on retry logic.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CommitExecutorTest {

    private static final long EPOCH = 1L;

    private Vertx vertx;

    @BeforeAll
    void setupAll() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    void tearDownAll() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    // ---- Happy path (no retry needed) -------------------------------------------

    @Test
    @DisplayName("Successful commit on first attempt — no retry")
    void successful_commit_no_retry() throws Exception {
        LuaCommitScript script  = mock(LuaCommitScript.class);
        CommitResult    okResult = new CommitResult(CommitStatus.OK, 1L, 1L, 1, 0);

        when(script.eval(any(), anyLong()))
                .thenReturn(Future.succeededFuture(okResult));

        CommitExecutor executor = new CommitExecutor(vertx, script, EPOCH, 2, 0);
        CommitResult   result   = await(executor.commit(plan()));

        assertThat(result.isOk()).isTrue();
        verify(script, times(1)).eval(any(), anyLong());
    }

    // ---- Retry on transient error -----------------------------------------------

    @Test
    @DisplayName("Transient error on first attempt → retried → success")
    void transient_error_retried_once_then_succeeds() throws Exception {
        LuaCommitScript script   = mock(LuaCommitScript.class);
        CommitResult    okResult = new CommitResult(CommitStatus.OK, 2L, 2L, 1, 0);
        AtomicInteger   calls    = new AtomicInteger(0);

        when(script.eval(any(), anyLong())).thenAnswer(inv -> {
            if (calls.getAndIncrement() == 0) {
                return Future.failedFuture(new RuntimeException("connection reset"));
            }
            return Future.succeededFuture(okResult);
        });

        CommitExecutor executor = new CommitExecutor(vertx, script, EPOCH, 2, 0);
        CommitResult   result   = await(executor.commit(plan()));

        assertThat(result.isOk()).isTrue();
        verify(script, times(2)).eval(any(), anyLong());
    }

    @Test
    @DisplayName("Transient error exhausts all retries → Future fails")
    void exhausted_retries_propagate_failure() throws Exception {
        LuaCommitScript script = mock(LuaCommitScript.class);
        when(script.eval(any(), anyLong()))
                .thenReturn(Future.failedFuture(new RuntimeException("redis unavailable")));

        CommitExecutor executor = new CommitExecutor(vertx, script, EPOCH, 2, 0);

        try {
            await(executor.commit(plan()));
            throw new AssertionError("Expected exception");
        } catch (Exception ex) {
            assertThat(ex.getCause()).hasMessageContaining("redis unavailable");
        }

        // initial attempt + 2 retries = 3 total calls
        verify(script, times(3)).eval(any(), anyLong());
    }

    // ---- Business outcomes are NOT retried --------------------------------------

    @Test
    @DisplayName("FENCED result is a success (not retried)")
    void fenced_result_is_not_retried() throws Exception {
        LuaCommitScript script      = mock(LuaCommitScript.class);
        CommitResult    fencedResult = new CommitResult(CommitStatus.FENCED, -1, -1, 0, 0);

        when(script.eval(any(), anyLong()))
                .thenReturn(Future.succeededFuture(fencedResult));

        CommitExecutor executor = new CommitExecutor(vertx, script, EPOCH, 2, 0);
        CommitResult   result   = await(executor.commit(plan()));

        assertThat(result.isFenced()).isTrue();
        verify(script, times(1)).eval(any(), anyLong());  // exactly once, no retry
    }

    @Test
    @DisplayName("DROPPED_RECENCY result is a success (not retried)")
    void dropped_recency_result_is_not_retried() throws Exception {
        LuaCommitScript script      = mock(LuaCommitScript.class);
        CommitResult    droppedResult = new CommitResult(CommitStatus.DROPPED_RECENCY, -1, -1, 0, 0);

        when(script.eval(any(), anyLong()))
                .thenReturn(Future.succeededFuture(droppedResult));

        CommitExecutor executor = new CommitExecutor(vertx, script, EPOCH, 2, 0);
        CommitResult   result   = await(executor.commit(plan()));

        assertThat(result.isRecencyDrop()).isTrue();
        verify(script, times(1)).eval(any(), anyLong());
    }

    // ---- Helpers ----------------------------------------------------------------

    private static WritePlan plan() {
        return new WritePlan("computers", "prod-test", 1000L,
                List.of(new SnapshotItem("item-1", 1L, "{}")),
                List.of());
    }

    private <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }
}
