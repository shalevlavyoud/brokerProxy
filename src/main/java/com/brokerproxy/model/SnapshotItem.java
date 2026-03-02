package com.brokerproxy.model;

import io.vertx.core.json.JsonObject;

/**
 * A single item inside a {@link Snapshot}.
 *
 * <p>Contract (from the system spec):
 * <ul>
 *   <li>{@code itemId}   — stable identifier for this item</li>
 *   <li>{@code version}  — strictly monotonic; any semantic change to the item bumps this</li>
 *   <li>{@code dataJson} — opaque payload stored verbatim in Redis; defaults to {@code "{}"}</li>
 * </ul>
 */
public record SnapshotItem(String itemId, long version, String dataJson) {

    public static SnapshotItem fromJson(JsonObject json) {
        return new SnapshotItem(
                json.getString("itemId"),
                json.getLong("version", 0L),
                json.getString("dataJson", "{}")
        );
    }

    public JsonObject toJson() {
        return new JsonObject()
                .put("itemId",   itemId)
                .put("version",  version)
                .put("dataJson", dataJson);
    }
}
