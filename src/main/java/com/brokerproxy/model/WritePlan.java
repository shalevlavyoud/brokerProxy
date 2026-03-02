package com.brokerproxy.model;

import java.util.List;

/**
 * Represents the set of Redis writes that a diff engine has determined are
 * necessary to synchronise state for a single {@link Snapshot}.
 *
 * <h3>Semantics</h3>
 * <ul>
 *   <li>{@code upserts} — items whose state ({@code bp:state:v}, {@code bp:state:j})
 *       and producer-version cache ({@code bp:prodver}) must be updated, and whose
 *       itemId must appear in the upserts change-index ({@code bp:ch:up}).</li>
 *   <li>{@code deletes} — itemIds that must be removed from state and cache, and
 *       whose itemId must appear in the deletes change-index ({@code bp:ch:del}).</li>
 * </ul>
 *
 * <p>An empty plan (no upserts, no deletes) means the snapshot was a NOOP —
 * all items were identical to the stored state.
 *
 * <p>The {@code topic}, {@code producerId}, and {@code msgTs} are carried here
 * so the Lua commit script (BE-06) has all context it needs in one object.
 */
public record WritePlan(
        String             topic,
        String             producerId,
        long               msgTs,
        List<SnapshotItem> upserts,
        List<String>       deletes) {

    /** Convenience factory — empty plan for a snapshot that produced no changes. */
    public static WritePlan noop(String topic, String producerId, long msgTs) {
        return new WritePlan(topic, producerId, msgTs, List.of(), List.of());
    }

    /** {@code true} when there are no writes to apply. */
    public boolean isEmpty() {
        return upserts.isEmpty() && deletes.isEmpty();
    }

    /** Human-readable summary for structured log lines. */
    public String summary() {
        return "upserts=" + upserts.size() + " deletes=" + deletes.size();
    }
}
