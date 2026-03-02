package com.brokerproxy.model;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link Snapshot} and {@link SnapshotItem} JSON round-trip parsing.
 * No Vert.x runtime required — pure data-model tests.
 */
class SnapshotParsingTest {

    // ---- Snapshot.fromJson ------------------------------------------------------

    @Test
    @DisplayName("Full valid snapshot is parsed correctly")
    void parse_full_snapshot() {
        JsonObject json = buildSnapshot("computers", "prod-1", 1_000L,
                List.of(buildItem("item-A", 3L, "{\"x\":1}")));

        Snapshot s = Snapshot.fromJson(json);

        assertThat(s.topic()).isEqualTo("computers");
        assertThat(s.producerId()).isEqualTo("prod-1");
        assertThat(s.msgTs()).isEqualTo(1_000L);
        assertThat(s.items()).hasSize(1);
        assertThat(s.items().get(0).itemId()).isEqualTo("item-A");
        assertThat(s.items().get(0).version()).isEqualTo(3L);
        assertThat(s.items().get(0).dataJson()).isEqualTo("{\"x\":1}");
    }

    @Test
    @DisplayName("Missing 'items' array defaults to empty list")
    void parse_missing_items_defaults_to_empty() {
        JsonObject json = new JsonObject()
                .put("topic", "headsets")
                .put("producerId", "prod-2")
                .put("msgTs", 500L);

        Snapshot s = Snapshot.fromJson(json);

        assertThat(s.items()).isEmpty();
    }

    @Test
    @DisplayName("Missing 'msgTs' defaults to 0")
    void parse_missing_msgTs_defaults_to_zero() {
        JsonObject json = new JsonObject()
                .put("topic", "conferences")
                .put("producerId", "prod-3");

        Snapshot s = Snapshot.fromJson(json);

        assertThat(s.msgTs()).isEqualTo(0L);
    }

    @Test
    @DisplayName("Large snapshot (500 items) is parsed completely")
    void parse_large_snapshot() {
        JsonArray items = new JsonArray();
        for (int i = 0; i < 500; i++) {
            items.add(buildItem("item-" + i, i, "{\"seq\":" + i + "}"));
        }
        JsonObject json = new JsonObject()
                .put("topic", "conferences")
                .put("producerId", "prod-big")
                .put("msgTs", 9_999L)
                .put("items", items);

        Snapshot s = Snapshot.fromJson(json);

        assertThat(s.items()).hasSize(500);
        assertThat(s.items().get(499).itemId()).isEqualTo("item-499");
        assertThat(s.items().get(499).version()).isEqualTo(499L);
    }

    @Test
    @DisplayName("Snapshot round-trips through toJson → fromJson identically")
    void snapshot_round_trip() {
        Snapshot original = Snapshot.fromJson(
                buildSnapshot("headsets", "prod-rt", 7777L,
                        List.of(buildItem("h-1", 2L, "{\"a\":\"b\"}"))));

        Snapshot restored = Snapshot.fromJson(original.toJson());

        assertThat(restored.topic()).isEqualTo(original.topic());
        assertThat(restored.producerId()).isEqualTo(original.producerId());
        assertThat(restored.msgTs()).isEqualTo(original.msgTs());
        assertThat(restored.items()).hasSize(1);
        assertThat(restored.items().get(0).itemId()).isEqualTo("h-1");
        assertThat(restored.items().get(0).dataJson()).isEqualTo("{\"a\":\"b\"}");
    }

    // ---- SnapshotItem.fromJson --------------------------------------------------

    @Test
    @DisplayName("Missing 'dataJson' defaults to '{}'")
    void item_missing_dataJson_defaults_to_empty_object() {
        JsonObject json = new JsonObject()
                .put("itemId", "x")
                .put("version", 1L);

        SnapshotItem item = SnapshotItem.fromJson(json);

        assertThat(item.dataJson()).isEqualTo("{}");
    }

    @Test
    @DisplayName("Missing 'version' defaults to 0")
    void item_missing_version_defaults_to_zero() {
        JsonObject json = new JsonObject()
                .put("itemId", "x")
                .put("dataJson", "{}");

        SnapshotItem item = SnapshotItem.fromJson(json);

        assertThat(item.version()).isEqualTo(0L);
    }

    @Test
    @DisplayName("Item round-trips through toJson → fromJson identically")
    void item_round_trip() {
        SnapshotItem original = new SnapshotItem("my-id", 42L, "{\"k\":\"v\"}");
        SnapshotItem restored = SnapshotItem.fromJson(original.toJson());

        assertThat(restored).isEqualTo(original);
    }

    // ---- Error cases ------------------------------------------------------------

    @Test
    @DisplayName("Malformed JSON string throws DecodeException")
    void malformed_json_throws() {
        assertThatThrownBy(() -> Snapshot.fromJson(new JsonObject("not-json-at-all}")))
                .isInstanceOf(Exception.class);
    }

    // ---- Builder helpers -------------------------------------------------------

    private static JsonObject buildSnapshot(String topic, String producerId, long msgTs,
                                             List<JsonObject> items) {
        JsonArray arr = new JsonArray();
        items.forEach(arr::add);
        return new JsonObject()
                .put("topic", topic)
                .put("producerId", producerId)
                .put("msgTs", msgTs)
                .put("items", arr);
    }

    private static JsonObject buildItem(String itemId, long version, String dataJson) {
        return new JsonObject()
                .put("itemId", itemId)
                .put("version", version)
                .put("dataJson", dataJson);
    }
}
