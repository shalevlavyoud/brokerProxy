package com.brokerproxy.service;

import com.brokerproxy.model.RecencyResult;
import com.brokerproxy.model.Snapshot;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces the per-{@code (topic, producerId)} recency invariant:
 * only snapshots with a strictly increasing {@code msgTs} are accepted.
 *
 * <h3>Redis key</h3>
 * <pre>
 *   bp:recency:{topic}:{producerId}  →  String (last accepted msgTs as decimal long)
 * </pre>
 * No TTL — these keys are permanent markers; they accumulate one entry per active
 * producer and are small (a few bytes each).
 *
 * <h3>Atomicity note</h3>
 * Recency is advanced atomically inside the Lua commit script (BE-06) together
 * with all state and change-index writes.  This class is read-only at runtime.
 */
public class RecencyGate {

    private static final Logger log = LoggerFactory.getLogger(RecencyGate.class);

    private final RedisAPI redis;
    private final String   prefix;

    public RecencyGate(RedisAPI redis, String prefix) {
        this.redis  = redis;
        this.prefix = prefix;
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Reads {@code bp:recency:{topic}:{producerId}} and checks whether
     * {@code snapshot.msgTs() > lastAccepted}.
     *
     * @return {@link RecencyResult#accepted()} when the key is absent (first snapshot)
     *         or {@code msgTs > lastAccepted}; {@link RecencyResult#dropped} otherwise.
     */
    public Future<RecencyResult> check(Snapshot snapshot) {
        String key = recencyKey(snapshot.topic(), snapshot.producerId());

        return redis.get(key)
                .map(response -> {
                    if (response == null) {
                        // No recency key yet — first snapshot for this producer
                        return RecencyResult.accepted();
                    }
                    long lastAccepted = Long.parseLong(response.toString());
                    if (snapshot.msgTs() <= lastAccepted) {
                        return RecencyResult.dropped(lastAccepted);
                    }
                    return RecencyResult.accepted();
                })
                .onFailure(err -> log.error(
                        "event=recency.check_error key={} error=\"{}\"", key, err.getMessage()));
    }

    // ---- Helpers ----------------------------------------------------------------

    /** {@code bp:recency:{topic}:{producerId}} */
    private String recencyKey(String topic, String producerId) {
        return prefix + ":recency:" + topic + ":" + producerId;
    }
}
