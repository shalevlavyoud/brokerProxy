package com.brokerproxy.service;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;

import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for per-topic diff engines.
 *
 * <p>Holds shared Redis infrastructure (client + key prefix) and common
 * utilities (key-name helpers, HGETALL response parser).  Concrete subclasses
 * implement the topic-specific {@link #diff} method and choose their own
 * Redis data structure for the producer-version cache.
 *
 * <h3>Subclasses</h3>
 * <ul>
 *   <li>{@link ComputersDiffEngine} — single-item producers; reads a
 *       <b>scalar</b> key {@code bp:compver:computers:{producerId}}</li>
 *   <li>{@link ProducerCacheDiffEngine} — multi-item producers (headsets /
 *       conferences); reads a <b>Hash</b> key
 *       {@code bp:prodver:{topic}:{producerId}}</li>
 * </ul>
 */
public abstract class AbstractDiffEngine {

    private final RedisAPI redis;
    private final String   prefix;
    /** Maximum allowed byte length of a single item's {@code dataJson} payload. */
    final int maxDataJsonBytes;

    /** Convenience constructor for tests — uses the default guardrail limit. */
    protected AbstractDiffEngine(RedisAPI redis, String prefix) {
        this(redis, prefix, AppConfig.DEFAULT_MAX_DATA_JSON_BYTES);
    }

    /** Full constructor for production use. */
    protected AbstractDiffEngine(RedisAPI redis, String prefix, int maxDataJsonBytes) {
        this.redis            = redis;
        this.prefix           = prefix;
        this.maxDataJsonBytes = maxDataJsonBytes;
    }

    /**
     * Computes a {@link WritePlan} for the given snapshot.
     *
     * <p>All I/O must be non-blocking; the returned {@link Future} completes
     * on the Vert.x event loop.
     */
    public abstract Future<WritePlan> diff(Snapshot snapshot);

    // ---- Protected accessors ----------------------------------------------------

    /** Returns the Redis API client. */
    protected RedisAPI redis() { return redis; }

    /** Returns the Redis key prefix (e.g. {@code "bp"}). */
    protected String prefix() { return prefix; }

    // ---- Key helpers ------------------------------------------------------------

    /**
     * Hash key for the multi-item producer version cache.
     * <pre>bp:prodver:{topic}:{producerId}</pre>
     * Used by {@link ProducerCacheDiffEngine} (headsets / conferences).
     */
    protected String prodverHashKey(String topic, String producerId) {
        return prefix + ":prodver:" + topic + ":" + producerId;
    }

    /**
     * Scalar key for the computers producer version cache.
     * <pre>bp:compver:computers:{producerId}</pre>
     * Value format: {@code "{itemId}|{version}"}.
     * Used by {@link ComputersDiffEngine}.
     */
    protected String compverScalarKey(String producerId) {
        return prefix + ":compver:computers:" + producerId;
    }

    // ---- Response parsers -------------------------------------------------------

    /**
     * Parses a Redis HGETALL {@link Response} into a plain {@code Map<String, Long>}.
     *
     * <p>HGETALL returns a flat bulk-string array: [field0, val0, field1, val1, …].
     * {@code Response.keys()} is NOT available in Vert.x 4.5.x; use index-based
     * iteration with {@code i += 2}.
     *
     * <p>The guard {@code i + 1 < response.size()} (rather than
     * {@code i < response.size() - 1}) is used to avoid any ambiguity with
     * corrupt odd-element responses: the loop requires both the field and its
     * paired value to be present before reading either.
     */
    protected static Map<String, Long> parseHgetall(Response response) {
        Map<String, Long> result = new HashMap<>();
        if (response == null || response.size() == 0) {
            return result;
        }
        for (int i = 0; i + 1 < response.size(); i += 2) {
            String field   = response.get(i).toString();
            long   version = Long.parseLong(response.get(i + 1).toString());
            result.put(field, version);
        }
        return result;
    }
}
