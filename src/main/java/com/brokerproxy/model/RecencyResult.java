package com.brokerproxy.model;

/**
 * Result of a recency-gate check for a single {@link Snapshot}.
 *
 * @param wasAccepted    {@code true} if the snapshot passed the gate
 * @param lastAcceptedTs the last accepted {@code msgTs} stored in Redis;
 *                       0 when no recency key exists (first-ever snapshot)
 */
public record RecencyResult(boolean wasAccepted, long lastAcceptedTs) {

    /** Snapshot passed — no stored recency key (first snapshot for this producer). */
    public static RecencyResult accepted() {
        return new RecencyResult(true, 0L);
    }

    /** Snapshot dropped — {@code msgTs <= lastAcceptedTs}. */
    public static RecencyResult dropped(long lastAcceptedTs) {
        return new RecencyResult(false, lastAcceptedTs);
    }
}
