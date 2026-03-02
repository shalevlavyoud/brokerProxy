package com.brokerproxy.service;

import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;

import java.util.Map;

/**
 * Diff engine for the {@code computers} topic — delegates to
 * {@link ProducerCacheDiffEngine} which contains all shared logic.
 *
 * <h3>Computers invariant</h3>
 * Each producer sends exactly one item per snapshot (single-item producer).
 * The baseline is stored in {@code bp:prodver:computers:{producerId}} as a
 * HASH with a single entry: {@code itemId → version}.
 *
 * <h3>Diff rules</h3>
 * See {@link ProducerCacheDiffEngine} — identical algorithm, tested there.
 * <ol>
 *   <li>Item not in cache → UPSERT (first snapshot)</li>
 *   <li>Same itemId, same version → NOOP</li>
 *   <li>Same itemId, higher version → UPSERT</li>
 *   <li>Different itemId → DELETE old + UPSERT new</li>
 *   <li>newVersion &lt; storedVersion → VERSION_ANOMALY: log warn, metric, skip</li>
 * </ol>
 */
public class ComputersDiffEngine {

    private final ProducerCacheDiffEngine delegate;

    public ComputersDiffEngine(RedisAPI redis, String prefix) {
        this.delegate = new ProducerCacheDiffEngine(redis, prefix);
    }

    /** Reads the producer cache and computes the {@link WritePlan}. */
    public Future<WritePlan> diff(Snapshot snapshot) {
        return delegate.diff(snapshot);
    }

    /**
     * Pure diff — no I/O. Package-private for unit tests in
     * {@code ComputersDiffEngineTest}.
     */
    WritePlan computeDiff(Snapshot snapshot, Map<String, Long> oldVersions) {
        return delegate.computeDiff(snapshot, oldVersions);
    }
}
