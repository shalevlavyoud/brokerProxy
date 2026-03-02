package com.brokerproxy.model;

/**
 * Result returned by the atomic Lua commit script (BE-06).
 *
 * @param status           outcome of the commit attempt
 * @param firstSeq         first SEQ allocated (−1 when no changes or non-OK)
 * @param lastSeq          last SEQ allocated  (−1 when no changes or non-OK)
 * @param appliedUpserts   number of upserts written to Redis
 * @param appliedDeletes   number of deletes written to Redis
 */
public record CommitResult(
        CommitStatus status,
        long         firstSeq,
        long         lastSeq,
        int          appliedUpserts,
        int          appliedDeletes) {

    /** Outcome of a Lua commit attempt. */
    public enum CommitStatus {
        /** All checks passed; state, indices, and recency written atomically. */
        OK,
        /** Epoch mismatch — this instance is stale; nothing was written. */
        FENCED,
        /** {@code msgTs <= lastAccepted} — snapshot is out-of-order; nothing was written. */
        DROPPED_RECENCY
    }

    public boolean isOk()       { return status == CommitStatus.OK; }
    public boolean isFenced()   { return status == CommitStatus.FENCED; }
    public boolean isRecencyDrop() { return status == CommitStatus.DROPPED_RECENCY; }
    public boolean hasChanges() { return appliedUpserts > 0 || appliedDeletes > 0; }
}
