package com.brokerproxy.service;

import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.model.CommitResult;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * Production-safe wrapper around {@link LuaCommitScript} that adds:
 * <ul>
 *   <li><b>Retry</b> — retries up to {@code maxRetries} times on transient
 *       Redis connection errors with configurable back-off delay.</li>
 *   <li><b>No-retry guard</b> — business outcomes ({@code FENCED},
 *       {@code DROPPED_RECENCY}) arrive as successful {@link CommitResult}
 *       values and are therefore never retried.</li>
 *   <li><b>Metrics</b> — records commit latency, status counter, and
 *       failure counter on every attempt.</li>
 * </ul>
 *
 * <h3>Retry policy</h3>
 * A commit attempt is retried when the underlying {@link Future} fails
 * (i.e. a Redis I/O error, not a business-logic rejection).
 * {@code NOSCRIPT} errors are handled transparently inside
 * {@link LuaCommitScript} and are invisible to this class.
 */
public class CommitExecutor {

    private static final Logger log = LoggerFactory.getLogger(CommitExecutor.class);

    private final Vertx            vertx;
    private final LuaCommitScript  script;
    private final long             leaderEpoch;
    private final int              maxRetries;
    private final int              retryDelayMs;
    /** Per-attempt timeout in ms; 0 means no timeout applied. */
    private final int              commitTimeoutMs;

    /**
     * Backward-compatible constructor — no per-attempt timeout.
     *
     * @param vertx         Vert.x instance (needed for retry timer)
     * @param script        the underlying Lua commit script handle
     * @param leaderEpoch   epoch passed to the Lua script for fencing
     * @param maxRetries    max number of retry attempts after the initial failure
     * @param retryDelayMs  ms to wait between retries (0 = immediate)
     */
    public CommitExecutor(Vertx vertx, LuaCommitScript script, long leaderEpoch,
                          int maxRetries, int retryDelayMs) {
        this(vertx, script, leaderEpoch, maxRetries, retryDelayMs, 0);
    }

    /**
     * Full constructor for production use.
     *
     * @param commitTimeoutMs per-attempt Redis timeout in ms; {@code 0} disables the
     *                        timeout (each attempt may wait indefinitely for Redis)
     */
    public CommitExecutor(Vertx vertx, LuaCommitScript script, long leaderEpoch,
                          int maxRetries, int retryDelayMs, int commitTimeoutMs) {
        this.vertx           = vertx;
        this.script          = script;
        this.leaderEpoch     = leaderEpoch;
        this.maxRetries      = maxRetries;
        this.retryDelayMs    = retryDelayMs;
        this.commitTimeoutMs = commitTimeoutMs;
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Commits the given {@link WritePlan} atomically via {@link LuaCommitScript},
     * with retry on transient Redis errors and Prometheus metric emission.
     *
     * @param plan the computed diff to persist
     * @return a {@link Future} that resolves to a {@link CommitResult};
     *         fails only if all retries are exhausted
     */
    public Future<CommitResult> commit(WritePlan plan) {
        long start = System.currentTimeMillis();

        return executeWithRetry(plan, maxRetries)
                .onSuccess(result -> {
                    long durationMs = System.currentTimeMillis() - start;
                    BrokerMetrics.recordCommitDuration(plan.topic(), durationMs);
                    BrokerMetrics.commitTotal(plan.topic(), result.status().name());
                    log.debug("event=commit_executor.done topic={} producerId={} "
                                    + "status={} durationMs={}",
                            plan.topic(), plan.producerId(),
                            result.status().name(), durationMs);
                })
                .onFailure(err -> {
                    BrokerMetrics.commitFailTotal(plan.topic(), "REDIS_ERROR");
                    log.error("event=commit_executor.exhausted topic={} producerId={} "
                                    + "maxRetries={} error=\"{}\"",
                            plan.topic(), plan.producerId(), maxRetries, err.getMessage());
                });
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Recursive retry helper.  On failure, schedules the next attempt after
     * {@link #retryDelayMs} using the Vert.x event loop (non-blocking).
     *
     * @param remaining number of retry attempts left (0 = propagate failure)
     */
    private Future<CommitResult> executeWithRetry(WritePlan plan, int remaining) {
        return withTimeout(script.eval(plan, leaderEpoch), plan)
                .recover(err -> {
                    if (remaining > 0) {
                        log.warn("event=commit_executor.retry topic={} producerId={} "
                                        + "retriesLeft={} error=\"{}\"",
                                plan.topic(), plan.producerId(),
                                remaining, err.getMessage());
                        return scheduleRetry(plan, remaining - 1);
                    }
                    return Future.failedFuture(err);
                });
    }

    /**
     * Schedules the next retry attempt after {@link #retryDelayMs} ms using
     * the Vert.x timer (keeps the event loop free during the delay).
     */
    private Future<CommitResult> scheduleRetry(WritePlan plan, int remaining) {
        if (retryDelayMs <= 0) {
            return executeWithRetry(plan, remaining);
        }
        Promise<CommitResult> promise = Promise.promise();
        vertx.setTimer(retryDelayMs, ignored ->
                executeWithRetry(plan, remaining).onComplete(promise));
        return promise.future();
    }

    /**
     * Wraps a commit {@link Future} with a configurable per-attempt timeout.
     *
     * <p>When {@code commitTimeoutMs <= 0}, returns {@code f} unchanged.
     * Otherwise, a Vert.x timer is armed; if the timer fires before the future
     * completes, the promise is failed with a {@link TimeoutException} and the
     * timer is cancelled on success/failure to avoid resource leaks.
     *
     * <p>Both the timer callback and the future callback run on the same
     * event-loop thread, so no synchronisation is required — only one branch
     * will ever win the {@code !promise.future().isComplete()} guard.
     */
    private Future<CommitResult> withTimeout(Future<CommitResult> f, WritePlan plan) {
        if (commitTimeoutMs <= 0) return f;

        Promise<CommitResult> promise = Promise.promise();
        long timerId = vertx.setTimer(commitTimeoutMs, id -> {
            if (!promise.future().isComplete()) {
                log.warn("event=commit_executor.timeout topic={} producerId={} commitTimeoutMs={}",
                        plan.topic(), plan.producerId(), commitTimeoutMs);
                promise.fail(new TimeoutException(
                        "Redis commit timed out after " + commitTimeoutMs + "ms"));
            }
        });
        f.onComplete(ar -> {
            vertx.cancelTimer(timerId);
            if (!promise.future().isComplete()) {
                promise.handle(ar);
            }
        });
        return promise.future();
    }
}
