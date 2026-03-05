package com.brokerproxy.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.vertx.micrometer.backends.BackendRegistries;

import java.util.concurrent.TimeUnit;

/**
 * Typed metric accessors for the Broker Proxy.
 *
 * <p>All methods are null-safe: if the Prometheus backend is not initialised
 * (e.g. in unit tests that don't configure Micrometer) every call is a no-op.
 *
 * <p>Metric naming follows Prometheus snake_case convention with the
 * {@code brokerproxy_} prefix.
 */
public final class BrokerMetrics {

    // ---- Metric name constants ---------------------------------------------------

    public static final String SNAPSHOTS_RECEIVED = "brokerproxy_snapshots_received_total";
    public static final String SNAPSHOTS_DROPPED  = "brokerproxy_snapshots_dropped_total";
    public static final String ITEMS_UPSERT       = "brokerproxy_items_upsert_total";
    public static final String ITEMS_DELETE        = "brokerproxy_items_delete_total";
    public static final String SNAPSHOTS_NOOP      = "brokerproxy_snapshots_noop_total";
    public static final String VERSION_ANOMALY     = "brokerproxy_version_anomaly_total";
    public static final String COMMIT_DURATION     = "brokerproxy_redis_commit_duration_seconds";
    public static final String COMMIT_TOTAL        = "brokerproxy_commit_total";
    public static final String COMMIT_FAIL_TOTAL   = "brokerproxy_commit_fail_total";
    public static final String LEADERSHIP_CHANGES  = "brokerproxy_leadership_changes_total";
    public static final String FENCING_DENIED      = "brokerproxy_fencing_denied_total";
    public static final String OVERSIZED_PAYLOAD   = "brokerproxy_oversized_payload_total";

    public static final String CHANGES_REQUESTS       = "brokerproxy_changes_requests_total";
    public static final String CHANGES_ITEMS_RETURNED = "brokerproxy_changes_items_returned_total";
    public static final String CHANGES_DURATION       = "brokerproxy_changes_request_duration_seconds";

    // Drop reason tags (used across multiple tasks)
    public static final String REASON_RECENCY      = "RECENCY";
    public static final String REASON_FENCED        = "FENCED";
    public static final String REASON_VERSION_ANOMALY = "VERSION_ANOMALY";
    public static final String REASON_BACKPRESSURE  = "BACKPRESSURE";
    public static final String REASON_REDIS_ERROR   = "REDIS_ERROR";

    private BrokerMetrics() {}

    // ---- Snapshot counters -------------------------------------------------------

    public static void snapshotReceived(String topic) {
        counter(SNAPSHOTS_RECEIVED, "topic", topic);
    }

    public static void snapshotDropped(String topic, String reason) {
        counter(SNAPSHOTS_DROPPED, "topic", topic, "reason", reason);
    }

    // ---- Item-level counters (populated in BE-04/05) -----------------------------

    public static void itemUpsert(String topic) {
        counter(ITEMS_UPSERT, "topic", topic);
    }

    /** Increments the upsert counter by {@code count} in a single call. */
    public static void itemUpsertCount(String topic, int count) {
        if (count <= 0) return;
        counterBy(ITEMS_UPSERT, count, "topic", topic);
    }

    public static void itemDelete(String topic) {
        counter(ITEMS_DELETE, "topic", topic);
    }

    /** Increments the delete counter by {@code count} in a single call. */
    public static void itemDeleteCount(String topic, int count) {
        if (count <= 0) return;
        counterBy(ITEMS_DELETE, count, "topic", topic);
    }

    /** Increments the oversized-payload drop counter for the given topic. */
    public static void oversizedPayload(String topic) {
        counter(OVERSIZED_PAYLOAD, "topic", topic);
    }

    public static void snapshotNoop(String topic) {
        counter(SNAPSHOTS_NOOP, "topic", topic);
    }

    public static void versionAnomaly(String topic) {
        counter(VERSION_ANOMALY, "topic", topic);
    }

    // ---- Redis commit histogram + counters (BE-07) ------------------------------

    /** Increments {@code bp_commit_total{topic, status}} on every commit attempt. */
    public static void commitTotal(String topic, String status) {
        counter(COMMIT_TOTAL, "topic", topic, "status", status);
    }

    /** Increments {@code bp_commit_fail_total{topic, cause}} on commit I/O failure. */
    public static void commitFailTotal(String topic, String cause) {
        counter(COMMIT_FAIL_TOTAL, "topic", topic, "cause", cause);
    }

    public static void recordCommitDuration(String topic, long durationMs) {
        MeterRegistry r = registry();
        if (r == null) return;
        Timer.builder(COMMIT_DURATION)
                .tag("topic", topic)
                .register(r)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    // ---- Changes read-path metrics (BE-08) --------------------------------------

    /** Increments {@code brokerproxy_changes_requests_total{topic, status=success|error}}. */
    public static void changesRequest(String topic, String status) {
        counter(CHANGES_REQUESTS, "topic", topic, "status", status);
    }

    /** Increments {@code brokerproxy_changes_items_returned_total{topic}} by {@code n}. */
    public static void changesItemsReturned(String topic, int n) {
        if (n <= 0) return;
        counterBy(CHANGES_ITEMS_RETURNED, n, "topic", topic);
    }

    /** Records the total request duration for {@code GET /changes} as a histogram. */
    public static void recordChangesDuration(String topic, long durationMs) {
        MeterRegistry r = registry();
        if (r == null) return;
        Timer.builder(CHANGES_DURATION)
                .tag("topic", topic)
                .register(r)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    // ---- Leadership / fencing (populated in leader-election task) ---------------

    public static void leadershipChange() {
        counter(LEADERSHIP_CHANGES);
    }

    public static void fencingDenied() {
        counter(FENCING_DENIED);
    }

    // ---- Internal helpers -------------------------------------------------------

    private static void counter(String name, String... tags) {
        MeterRegistry r = registry();
        if (r == null) return;
        Counter.builder(name)
                .tags(tags)
                .register(r)
                .increment();
    }

    /** Increments a counter by a given amount — used for bulk operations. */
    private static void counterBy(String name, double amount, String... tags) {
        MeterRegistry r = registry();
        if (r == null) return;
        Counter.builder(name)
                .tags(tags)
                .register(r)
                .increment(amount);
    }

    private static MeterRegistry registry() {
        return BackendRegistries.getDefaultNow();
    }
}
