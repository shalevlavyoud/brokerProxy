package com.brokerproxy.model;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Immutable model for a full-snapshot message received from ActiveMQ.
 *
 * <p>Wire format (JSON text body on the JMS TextMessage):
 * <pre>
 * {
 *   "topic":      "computers",
 *   "producerId": "prod-42",
 *   "msgTs":      1700000000000,
 *   "items": [
 *     { "itemId": "item-1", "version": 7, "dataJson": "{...}" }
 *   ]
 * }
 * </pre>
 *
 * <p>If {@code topic} is absent in the JSON body it is filled from the JMS destination
 * name (the authoritative source) by {@link com.brokerproxy.verticle.JmsConsumerVerticle}.
 */
public record Snapshot(
        String topic,
        String producerId,
        long   msgTs,
        List<SnapshotItem> items) {

    public static Snapshot fromJson(JsonObject json) {
        String     topic      = json.getString("topic");
        String     producerId = json.getString("producerId");
        long       msgTs      = json.getLong("msgTs", 0L);
        JsonArray  itemsArr   = json.getJsonArray("items", new JsonArray());

        List<SnapshotItem> items = itemsArr.stream()
                .map(o -> SnapshotItem.fromJson((JsonObject) o))
                .collect(Collectors.toUnmodifiableList());

        return new Snapshot(topic, producerId, msgTs, items);
    }

    public JsonObject toJson() {
        JsonArray arr = new JsonArray();
        items.forEach(item -> arr.add(item.toJson()));

        return new JsonObject()
                .put("topic",      topic)
                .put("producerId", producerId)
                .put("msgTs",      msgTs)
                .put("items",      arr);
    }
}
