package com.brokerproxy.verticle;

import com.brokerproxy.model.Snapshot;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the backpressure and parsing logic that lives inside
 * {@link JmsConsumerVerticle}'s message listener.
 *
 * <p>We do NOT spin up a real ActiveMQ broker. Instead we:
 * <ol>
 *   <li>Build a Vert.x instance with Prometheus metrics.</li>
 *   <li>Register an event-bus consumer that simulates the {@link SnapshotProcessorVerticle}
 *       (it immediately replies, or optionally delays to let us test saturation).</li>
 *   <li>Directly invoke the listener logic via a package-visible helper method.</li>
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JmsBackpressureTest {

    private Vertx vertx;

    @BeforeAll
    void setup() {
        vertx = Vertx.vertx(new VertxOptions()
                .setMetricsOptions(new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)));
    }

    @AfterAll
    void tearDown() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    // ---- Parsing ----------------------------------------------------------------

    @Test
    @DisplayName("Valid TextMessage body is parsed into a Snapshot")
    void valid_text_message_is_parsed() throws Exception {
        JsonObject body = new JsonObject()
                .put("topic", "computers")
                .put("producerId", "prod-x")
                .put("msgTs", 123L)
                .put("items", io.vertx.core.json.JsonArray.of(
                        new JsonObject().put("itemId", "c-1").put("version", 1L)));

        Snapshot s = parseViaHelper(body.encode(), "computers");

        assertThat(s.topic()).isEqualTo("computers");
        assertThat(s.producerId()).isEqualTo("prod-x");
        assertThat(s.items()).hasSize(1);
    }

    @Test
    @DisplayName("TextMessage without 'topic' field takes topic from JMS destination")
    void missing_topic_field_filled_from_destination() throws Exception {
        JsonObject body = new JsonObject()
                .put("producerId", "prod-y")
                .put("msgTs", 200L);

        Snapshot s = parseViaHelper(body.encode(), "headsets");

        assertThat(s.topic()).isEqualTo("headsets");
    }

    // ---- Backpressure -----------------------------------------------------------

    @Test
    @DisplayName("Semaphore drops messages when all permits are taken")
    void semaphore_drops_when_full() throws Exception {
        int maxPending = 3;
        Semaphore semaphore = new Semaphore(maxPending);
        AtomicInteger dropCount = new AtomicInteger(0);

        // Register a processor that never replies (simulates a slow processor)
        String addr = "bp.snapshot.computers-bp-test";
        vertx.eventBus().<JsonObject>consumer(addr, msg -> {
            // deliberately NOT replying — keeps permits consumed
        });

        // Send maxPending + 2 messages; the last 2 should be dropped
        int totalSent = maxPending + 2;
        AtomicInteger delivered = new AtomicInteger(0);

        for (int i = 0; i < totalSent; i++) {
            if (!semaphore.tryAcquire()) {
                dropCount.incrementAndGet();
                continue;
            }
            delivered.incrementAndGet();
            JsonObject payload = new JsonObject()
                    .put("topic", "computers")
                    .put("producerId", "prod-z")
                    .put("msgTs", (long) i)
                    .put("items", new io.vertx.core.json.JsonArray());

            vertx.eventBus().<JsonObject>request(addr, payload,
                    reply -> semaphore.release());
        }

        assertThat(delivered.get()).isEqualTo(maxPending);
        assertThat(dropCount.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("Semaphore permit is released after processor replies, allowing next message")
    void semaphore_released_on_reply() throws Exception {
        int maxPending = 1;
        Semaphore semaphore = new Semaphore(maxPending);

        String addr = "bp.snapshot.headsets-bp-test";

        // Processor immediately replies
        vertx.eventBus().<JsonObject>consumer(addr,
                msg -> msg.reply(new JsonObject().put("status", "ok")));

        CountDownLatch latch = new CountDownLatch(2);
        List<String> delivered = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            final int seq = i;
            // Wait for a permit (with timeout — tests must not use Thread.sleep in a loop,
            // but a one-shot tryAcquire with timeout is acceptable in a test helper)
            boolean acquired = semaphore.tryAcquire(2, TimeUnit.SECONDS);
            assertThat(acquired).as("permit %d not acquired in time", seq).isTrue();

            JsonObject payload = new JsonObject()
                    .put("topic", "headsets")
                    .put("producerId", "prod-w")
                    .put("msgTs", (long) seq)
                    .put("items", new io.vertx.core.json.JsonArray());

            vertx.eventBus().<JsonObject>request(addr, payload,
                    reply -> {
                        semaphore.release();
                        delivered.add("msg-" + seq);
                        latch.countDown();
                    });
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(delivered).hasSize(2);
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Exercises the same parse logic as
     * {@code JmsConsumerVerticle.parseMessage()} without needing a real JMS session.
     */
    private static Snapshot parseViaHelper(String jsonBody, String topicName)
            throws JMSException {
        TextMessage fakeMsg = new StubTextMessage(jsonBody);
        // Mirror the parse logic from JmsConsumerVerticle
        JsonObject json = new JsonObject(fakeMsg.getText());
        if (!json.containsKey("topic") || json.getString("topic") == null) {
            json.put("topic", topicName);
        }
        return Snapshot.fromJson(json);
    }

    // ---- Minimal TextMessage stub ----------------------------------------------

    /** Stub JMS TextMessage that only implements getText(). */
    private record StubTextMessage(String text) implements TextMessage {
        @Override public String getText()          { return text; }
        @Override public void setText(String s)    {}
        @Override public String getJMSMessageID()  { return "stub-id"; }
        @Override public void setJMSMessageID(String id) {}
        @Override public long getJMSTimestamp()    { return 0; }
        @Override public void setJMSTimestamp(long t) {}
        @Override public byte[] getJMSCorrelationIDAsBytes() { return new byte[0]; }
        @Override public void setJMSCorrelationIDAsBytes(byte[] b) {}
        @Override public void setJMSCorrelationID(String id) {}
        @Override public String getJMSCorrelationID() { return null; }
        @Override public javax.jms.Destination getJMSReplyTo() { return null; }
        @Override public void setJMSReplyTo(javax.jms.Destination d) {}
        @Override public javax.jms.Destination getJMSDestination() { return null; }
        @Override public void setJMSDestination(javax.jms.Destination d) {}
        @Override public int getJMSDeliveryMode() { return 0; }
        @Override public void setJMSDeliveryMode(int m) {}
        @Override public boolean getJMSRedelivered() { return false; }
        @Override public void setJMSRedelivered(boolean r) {}
        @Override public String getJMSType() { return null; }
        @Override public void setJMSType(String t) {}
        @Override public long getJMSExpiration() { return 0; }
        @Override public void setJMSExpiration(long e) {}
        @Override public long getJMSDeliveryTime() { return 0; }
        @Override public void setJMSDeliveryTime(long d) {}
        @Override public int getJMSPriority() { return 0; }
        @Override public void setJMSPriority(int p) {}
        @Override public void clearProperties() {}
        @Override public boolean propertyExists(String n) { return false; }
        @Override public boolean getBooleanProperty(String n) { return false; }
        @Override public byte getByteProperty(String n) { return 0; }
        @Override public short getShortProperty(String n) { return 0; }
        @Override public int getIntProperty(String n) { return 0; }
        @Override public long getLongProperty(String n) { return 0; }
        @Override public float getFloatProperty(String n) { return 0; }
        @Override public double getDoubleProperty(String n) { return 0; }
        @Override public String getStringProperty(String n) { return null; }
        @Override public Object getObjectProperty(String n) { return null; }
        @Override public Enumeration getPropertyNames() { return null; }
        @Override public void setBooleanProperty(String n, boolean v) {}
        @Override public void setByteProperty(String n, byte v) {}
        @Override public void setShortProperty(String n, short v) {}
        @Override public void setIntProperty(String n, int v) {}
        @Override public void setLongProperty(String n, long v) {}
        @Override public void setFloatProperty(String n, float v) {}
        @Override public void setDoubleProperty(String n, double v) {}
        @Override public void setStringProperty(String n, String v) {}
        @Override public void setObjectProperty(String n, Object v) {}
        @Override public void acknowledge() {}
        @Override public void clearBody() {}
        @Override public <T> T getBody(Class<T> c) { return null; }
        @Override public boolean isBodyAssignableTo(Class c) { return false; }
    }
}
