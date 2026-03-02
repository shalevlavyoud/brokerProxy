package com.brokerproxy.verticle;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.model.Snapshot;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Vert.x verticle that owns the ActiveMQ JMS connection and one consumer per topic.
 *
 * <h3>Threading model</h3>
 * <ul>
 *   <li>JMS setup (blocking API) runs inside {@code vertx.executeBlocking()}.
 *   <li>JMS {@code MessageListener.onMessage()} is called on the ActiveMQ internal
 *       session-delivery thread — NOT the Vert.x event loop.
 *   <li>From the JMS thread we call {@code vertx.eventBus().request()} which is
 *       thread-safe and hands the message to the event loop asynchronously.
 * </ul>
 *
 * <h3>Backpressure</h3>
 * Each topic has a {@link Semaphore} of size {@code jmsMaxPendingPerTopic}.
 * A permit is acquired before publishing to the event bus and released when the
 * {@link SnapshotProcessorVerticle} replies.  If no permit is available the snapshot
 * is <strong>dropped</strong> and {@code brokerproxy_snapshots_dropped_total{reason="BACKPRESSURE"}}
 * is incremented — preventing unbounded heap growth under burst.
 *
 * <h3>ActiveMQ prefetch</h3>
 * {@code topicPrefetch} is set on the connection factory so the broker will not push
 * more than that many unacknowledged messages per consumer, providing a second layer
 * of flow-control at the transport level.
 */
public class JmsConsumerVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(JmsConsumerVerticle.class);

    /** Event-bus address prefix; full address = {@code SNAPSHOT_ADDR + topicName}. */
    public static final String SNAPSHOT_ADDR = "bp.snapshot.";

    private Connection  jmsConnection;
    private AppConfig   config;
    private final List<Session> sessions = new ArrayList<>();

    // ---- Lifecycle --------------------------------------------------------------

    @Override
    public void start(Promise<Void> startPromise) {
        config = AppConfig.from(context.config());

        vertx.executeBlocking(this::setupJms)
                .onSuccess(conn -> {
                    this.jmsConnection = conn;
                    log.info("event=jms.connected verticle=JmsConsumerVerticle "
                                    + "url={} topics={}",
                            config.activemqBrokerUrl(), config.topics());
                    startPromise.complete();
                })
                .onFailure(err -> {
                    log.error("event=jms.connect_failed verticle=JmsConsumerVerticle "
                                    + "url={} error=\"{}\"",
                            config.activemqBrokerUrl(), err.getMessage(), err);
                    startPromise.fail(err);
                });
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        vertx.executeBlocking(() -> {
            sessions.forEach(s -> {
                try { s.close(); } catch (JMSException ignored) {}
            });
            if (jmsConnection != null) jmsConnection.close();
            return null;
        }).onComplete(ar -> {
            log.info("event=jms.disconnected verticle=JmsConsumerVerticle");
            stopPromise.complete();
        });
    }

    // ---- JMS setup (runs in executeBlocking worker thread) ----------------------

    private Connection setupJms() throws JMSException {
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory(config.activemqBrokerUrl());

        // Broker-side flow control: limit unacknowledged messages pushed to this client
        factory.getPrefetchPolicy().setTopicPrefetch(config.jmsTopicPrefetch());
        // Disable optimise-acknowledge so CLIENT_ACKNOWLEDGE works correctly
        factory.setOptimizeAcknowledge(false);

        Connection conn = factory.createConnection();
        conn.setExceptionListener(this::handleJmsException);

        for (String topicName : config.topics()) {
            // Each topic gets its own session (JMS sessions are single-threaded)
            Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            sessions.add(session);

            Topic dest = session.createTopic(topicName);
            MessageConsumer consumer = session.createConsumer(dest);

            // Per-topic backpressure state
            Semaphore    semaphore   = new Semaphore(config.jmsMaxPendingPerTopic());
            AtomicLong   dropCounter = new AtomicLong(0);

            consumer.setMessageListener(
                    msg -> onMessage(msg, topicName, semaphore, dropCounter));
        }

        conn.start();
        return conn;
    }

    // ---- Message listener (runs on ActiveMQ internal delivery thread) -----------

    private void onMessage(Message msg, String topicName,
                            Semaphore semaphore, AtomicLong dropCounter) {
        BrokerMetrics.snapshotReceived(topicName);

        // ---- Backpressure gate --------------------------------------------------
        if (!semaphore.tryAcquire()) {
            long dropped = dropCounter.incrementAndGet();
            // Log at warn every first drop and every 100th to avoid log spam
            if (dropped == 1 || dropped % 100 == 0) {
                log.warn("event=jms.backpressure_drop verticle=JmsConsumerVerticle "
                                + "topic={} totalDropped={}",
                        topicName, dropped);
            }
            BrokerMetrics.snapshotDropped(topicName, BrokerMetrics.REASON_BACKPRESSURE);
            silentAck(msg);
            return;
        }

        // ---- Parse --------------------------------------------------------------
        Snapshot snapshot;
        try {
            snapshot = parseMessage(msg, topicName);
        } catch (Exception e) {
            semaphore.release();
            log.error("event=jms.parse_failed verticle=JmsConsumerVerticle "
                            + "topic={} error=\"{}\"",
                    topicName, e.getMessage(), e);
            BrokerMetrics.snapshotDropped(topicName, BrokerMetrics.REASON_REDIS_ERROR);
            silentAck(msg);
            return;
        }

        // ---- Hand off to event loop via event bus (thread-safe) -----------------
        DeliveryOptions opts = new DeliveryOptions()
                .setSendTimeout(config.jmsProcessingTimeoutMs());

        vertx.eventBus().<JsonObject>request(
                SNAPSHOT_ADDR + topicName,
                snapshot.toJson(),
                opts,
                reply -> {
                    // Release permit when the processor verticle has acknowledged
                    semaphore.release();
                    if (reply.failed()) {
                        log.warn("event=jms.snapshot_undelivered "
                                        + "verticle=JmsConsumerVerticle "
                                        + "topic={} producerId={} msgTs={} error=\"{}\"",
                                topicName, snapshot.producerId(), snapshot.msgTs(),
                                reply.cause().getMessage());
                    }
                });

        silentAck(msg);
    }

    // ---- Helpers ----------------------------------------------------------------

    private static Snapshot parseMessage(Message msg, String topicName) throws JMSException {
        if (!(msg instanceof TextMessage)) {
            throw new IllegalArgumentException(
                    "Expected TextMessage but got " + msg.getClass().getSimpleName());
        }
        String body = ((TextMessage) msg).getText();
        JsonObject json = new JsonObject(body);

        // JMS destination name is the authoritative topic; fill in if absent from body
        if (!json.containsKey("topic") || json.getString("topic") == null) {
            json.put("topic", topicName);
        }
        return Snapshot.fromJson(json);
    }

    private static void silentAck(Message msg) {
        try { msg.acknowledge(); } catch (JMSException ignored) {}
    }

    private void handleJmsException(JMSException ex) {
        log.error("event=jms.connection_error verticle=JmsConsumerVerticle error=\"{}\"",
                ex.getMessage(), ex);
        // Reconnect attempt after a short back-off; a real reconnect loop is
        // implemented in BE-07 (commit wrapper covers Redis; JMS reconnect uses
        // ActiveMQ failover transport in production: failover:(tcp://...) )
        vertx.setTimer(5_000, id ->
                log.warn("event=jms.reconnect_pending verticle=JmsConsumerVerticle "
                        + "hint=\"switch brokerUrl to failover:(tcp://...) for auto-reconnect\""));
    }
}
