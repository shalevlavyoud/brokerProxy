package com.brokerproxy.service;

import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.SnapshotItem;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Diff engine for the {@code computers} topic.
 *
 * <p>Each computer producer owns <b>exactly one item</b> at a time.  The
 * baseline is stored as a single scalar Redis string:
 * <pre>
 *   bp:compver:computers:{producerId}  →  String "{itemId}|{version}"
 * </pre>
 * A single {@code GET} is sufficient — no {@code HGETALL} needed.
 * (No TTL — permanent while the producer is active.)
 *
 * <h3>Cases</h3>
 * <ul>
 *   <li>No baseline (first snapshot) → <b>UPSERT</b></li>
 *   <li>Same itemId, same version → <b>NOOP</b></li>
 *   <li>Same itemId, higher version → <b>UPSERT</b></li>
 *   <li>Same itemId, lower version → <b>VERSION_ANOMALY</b>: log warn + metric, NOOP</li>
 *   <li>Different itemId (ownership transfer) → <b>DELETE</b> old + <b>UPSERT</b> new</li>
 *   <li>Empty snapshot with existing baseline → <b>DELETE</b> old item</li>
 *   <li>dataJson exceeds {@code maxDataJsonBytes} → <b>OVERSIZED</b>: log warn + metric, NOOP</li>
 * </ul>
 *
 * <h3>Atomicity note</h3>
 * This class only <em>reads</em> from Redis.  The Lua commit script (BE-06)
 * writes the new scalar value atomically via {@code SET KEYS[8] itemId|version}
 * (or {@code DEL KEYS[8]} for a pure delete) alongside all state and index updates.
 */
public class ComputersDiffEngine extends AbstractDiffEngine {

    private static final Logger log = LoggerFactory.getLogger(ComputersDiffEngine.class);

    static final String TOPIC = "computers";

    /** Convenience constructor for tests — uses the default guardrail limit. */
    public ComputersDiffEngine(RedisAPI redis, String prefix) {
        super(redis, prefix);
    }

    /** Full constructor for production use. */
    public ComputersDiffEngine(RedisAPI redis, String prefix, int maxDataJsonBytes) {
        super(redis, prefix, maxDataJsonBytes);
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Reads the scalar baseline key and computes the {@link WritePlan}.
     *
     * <p>All I/O is non-blocking. The returned {@link Future} completes on the
     * Vert.x event loop — no blocking calls inside.
     */
    @Override
    public Future<WritePlan> diff(Snapshot snapshot) {
        String key = compverScalarKey(snapshot.producerId());

        return redis().get(key)
                .map(response -> computeDiff(snapshot, parseScalar(response)))
                .onFailure(err -> log.error(
                        "event=computers_diff.cache_read_error producerId={} error=\"{}\"",
                        snapshot.producerId(), err.getMessage()));
    }

    // ---- Package-private for unit testing ---------------------------------------

    /**
     * Pure diff computation — no I/O.
     *
     * <p>Exposed package-private so unit tests can exercise all golden cases
     * without a real Redis instance.
     *
     * @param snapshot incoming snapshot (should contain exactly 1 item for computers)
     * @param cached   the cached {@link CachedEntry}; {@code null} if no baseline yet
     * @return the computed {@link WritePlan}
     */
    WritePlan computeDiff(Snapshot snapshot, CachedEntry cached) {
        List<SnapshotItem> items      = snapshot.items();
        String             producerId = snapshot.producerId();

        // --- Empty snapshot: producer no longer owns any item --------------------
        if (items.isEmpty()) {
            if (cached != null) {
                log.info("event=computers_diff.delete_all producerId={} itemId={}",
                        producerId, cached.itemId());
                BrokerMetrics.itemDelete(TOPIC);
                return new WritePlan(TOPIC, producerId, snapshot.msgTs(),
                        List.of(), List.of(cached.itemId()));
            }
            return WritePlan.noop(TOPIC, producerId, snapshot.msgTs());
        }

        if (items.size() > 1) {
            log.warn("event=computers_diff.multi_item_snapshot producerId={} count={}"
                            + " — processing first item only",
                    producerId, items.size());
        }

        SnapshotItem incoming = items.get(0);

        // --- No baseline: first time we see this producer ------------------------
        if (cached == null) {
            if (!checkPayloadSize(incoming, producerId)) {
                return WritePlan.noop(TOPIC, producerId, snapshot.msgTs());
            }
            log.debug("event=computers_diff.upsert_new producerId={} itemId={} version={}",
                    producerId, incoming.itemId(), incoming.version());
            BrokerMetrics.itemUpsert(TOPIC);
            return new WritePlan(TOPIC, producerId, snapshot.msgTs(),
                    List.of(incoming), List.of());
        }

        // --- Ownership transfer: different computer assigned to this producer ----
        if (!incoming.itemId().equals(cached.itemId())) {
            if (!checkPayloadSize(incoming, producerId)) {
                return WritePlan.noop(TOPIC, producerId, snapshot.msgTs());
            }
            log.info("event=computers_diff.ownership_transfer producerId={} "
                            + "oldItemId={} newItemId={} newVersion={}",
                    producerId, cached.itemId(), incoming.itemId(), incoming.version());
            BrokerMetrics.itemDelete(TOPIC);
            BrokerMetrics.itemUpsert(TOPIC);
            return new WritePlan(TOPIC, producerId, snapshot.msgTs(),
                    List.of(incoming), List.of(cached.itemId()));
        }

        // --- Same item: compare versions -----------------------------------------
        if (incoming.version() > cached.version()) {
            if (!checkPayloadSize(incoming, producerId)) {
                return WritePlan.noop(TOPIC, producerId, snapshot.msgTs());
            }
            log.debug("event=computers_diff.upsert_higher producerId={} itemId={} "
                            + "oldVersion={} newVersion={}",
                    producerId, incoming.itemId(), cached.version(), incoming.version());
            BrokerMetrics.itemUpsert(TOPIC);
            return new WritePlan(TOPIC, producerId, snapshot.msgTs(),
                    List.of(incoming), List.of());
        }

        if (incoming.version() == cached.version()) {
            log.debug("event=computers_diff.noop producerId={} itemId={} version={}",
                    producerId, incoming.itemId(), incoming.version());
            BrokerMetrics.snapshotNoop(TOPIC);
            return WritePlan.noop(TOPIC, producerId, snapshot.msgTs());
        }

        // incoming.version() < cached.version() — anomaly
        log.warn("event=computers_diff.version_anomaly producerId={} itemId={} "
                        + "storedVersion={} incomingVersion={}",
                producerId, incoming.itemId(), cached.version(), incoming.version());
        BrokerMetrics.versionAnomaly(TOPIC);
        return WritePlan.noop(TOPIC, producerId, snapshot.msgTs());
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Returns {@code true} if the item's {@code dataJson} is within the allowed
     * limit; logs a warning and emits a metric if it exceeds the limit.
     */
    private boolean checkPayloadSize(SnapshotItem item, String producerId) {
        if (item.dataJson() != null && item.dataJson().length() > maxDataJsonBytes) {
            log.warn("event=computers_diff.oversized_payload producerId={} itemId={} "
                            + "dataJsonBytes={} max={}",
                    producerId, item.itemId(), item.dataJson().length(), maxDataJsonBytes);
            BrokerMetrics.oversizedPayload(TOPIC);
            return false;
        }
        return true;
    }

    /**
     * Parses the scalar Redis value {@code "{itemId}|{version}"} into a
     * {@link CachedEntry}, or {@code null} if the key is absent.
     *
     * <p>Uses {@code lastIndexOf('|')} so itemIds that contain {@code '|'}
     * are handled correctly (version is always a trailing number).
     */
    static CachedEntry parseScalar(Response response) {
        if (response == null) {
            return null;
        }
        String value = response.toString();
        int    sep   = value.lastIndexOf('|');
        if (sep < 0) {
            throw new IllegalStateException(
                    "Malformed compver scalar (expected 'itemId|version'): " + value);
        }
        String itemId  = value.substring(0, sep);
        long   version = Long.parseLong(value.substring(sep + 1));
        return new CachedEntry(itemId, version);
    }

    // ---- Nested types -----------------------------------------------------------

    /**
     * Immutable snapshot of the cached computer item baseline.
     *
     * <p>Corresponds to the scalar Redis value {@code "{itemId}|{version}"}
     * stored at {@code bp:compver:computers:{producerId}}.
     */
    record CachedEntry(String itemId, long version) {}
}
