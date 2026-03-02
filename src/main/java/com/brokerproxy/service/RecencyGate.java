package com.brokerproxy.service;

import com.brokerproxy.model.RecencyResult;
import com.brokerproxy.model.Snapshot;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
 * {@link #updateRecency} performs a non-atomic {@code SET}.  This is intentional
 * for BE-03: the Lua commit script (BE-06) will subsume this write so that recency
 * is updated atomically with state and change-index updates.
 * <p>
 * <strong>TODO BE-06:</strong> remove {@link #updateRecency} from the Java call-site
 * in {@code SnapshotProcessorVerticle} once it is part of the Lua script.
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

    /**
     * Writes {@code SET bp:recency:{topic}:{producerId} {msgTs}}.
     *
     * <p><strong>TEMPORARY — BE-03 only.</strong>  In BE-06 this write will be
     * removed from Java and performed inside the atomic Lua commit script so that
     * recency is never advanced unless the full state+index commit succeeds.
     */
    public Future<Void> updateRecency(Snapshot snapshot) {
        String key = recencyKey(snapshot.topic(), snapshot.producerId());
        return redis.set(List.of(key, String.valueOf(snapshot.msgTs())))
                .mapEmpty();
    }

    // ---- Helpers ----------------------------------------------------------------

    /** {@code bp:recency:{topic}:{producerId}} */
    private String recencyKey(String topic, String producerId) {
        return prefix + ":recency:" + topic + ":" + producerId;
    }
}
