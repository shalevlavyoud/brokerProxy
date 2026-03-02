package com.brokerproxy.verticle;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.model.RecencyResult;
import com.brokerproxy.model.Snapshot;
import com.brokerproxy.redis.RedisProvider;
import com.brokerproxy.service.RecencyGate;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event-loop verticle that receives parsed {@link Snapshot} objects from the
 * event bus and drives the processing pipeline.
 *
 * <h3>Current pipeline (BE-03)</h3>
 * <ol>
 *   <li>Receive snapshot JSON from {@code bp.snapshot.{topic}}</li>
 *   <li>Recency gate — drop if {@code msgTs <= lastAccepted} stored in Redis</li>
 *   <li>Log accept/drop with structured fields</li>
 *   <li>Reply to release the {@link JmsConsumerVerticle} backpressure semaphore</li>
 * </ol>
 *
 * <h3>Future pipeline extensions</h3>
 * <ul>
 *   <li>BE-04 — computers diff engine</li>
 *   <li>BE-05 — headsets / conferences diff engine</li>
 *   <li>BE-06 — atomic Lua commit to Redis (recency write moved there)</li>
 * </ul>
 */
public class SnapshotProcessorVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(SnapshotProcessorVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) {
        AppConfig    config      = AppConfig.from(context.config());
        RedisAPI     redisApi    = RedisProvider.createApi(vertx, config);
        RecencyGate  recencyGate = new RecencyGate(redisApi, config.redisPrefix());

        for (String topicName : config.topics()) {
            vertx.eventBus().<JsonObject>consumer(
                    JmsConsumerVerticle.SNAPSHOT_ADDR + topicName,
                    msg -> processSnapshot(msg, recencyGate)
            );
        }

        log.info("event=snapshot_processor.started verticle=SnapshotProcessorVerticle "
                + "topics={}", config.topics());
        startPromise.complete();
    }

    // ---- Processing pipeline ----------------------------------------------------

    private void processSnapshot(Message<JsonObject> msg, RecencyGate recencyGate) {
        Snapshot snapshot = Snapshot.fromJson(msg.body());

        recencyGate.check(snapshot)
                .onSuccess(result -> handleRecencyResult(msg, snapshot, recencyGate, result))
                .onFailure(err -> {
                    log.error("event=recency.check_failed topic={} producerId={} "
                                    + "msgTs={} error=\"{}\"",
                            snapshot.topic(), snapshot.producerId(),
                            snapshot.msgTs(), err.getMessage(), err);
                    BrokerMetrics.snapshotDropped(snapshot.topic(),
                            BrokerMetrics.REASON_REDIS_ERROR);
                    // Reply so the JMS backpressure semaphore is released
                    msg.reply(new JsonObject()
                            .put("status", "dropped")
                            .put("reason", BrokerMetrics.REASON_REDIS_ERROR));
                });
    }

    private void handleRecencyResult(Message<JsonObject> msg, Snapshot snapshot,
                                      RecencyGate recencyGate, RecencyResult result) {
        if (!result.wasAccepted()) {
            // ---- Drop -----------------------------------------------------------
            log.info("event=snapshot.dropped topic={} producerId={} msgTs={} "
                            + "lastAcceptedTs={} reason=RECENCY",
                    snapshot.topic(), snapshot.producerId(),
                    snapshot.msgTs(), result.lastAcceptedTs());
            BrokerMetrics.snapshotDropped(snapshot.topic(), BrokerMetrics.REASON_RECENCY);
            msg.reply(new JsonObject()
                    .put("status", "dropped")
                    .put("reason", BrokerMetrics.REASON_RECENCY));
            return;
        }

        // ---- Accept -------------------------------------------------------------
        log.info("event=snapshot.accepted topic={} producerId={} msgTs={} itemCount={}",
                snapshot.topic(), snapshot.producerId(),
                snapshot.msgTs(), snapshot.items().size());

        BrokerMetrics.snapshotReceived(snapshot.topic());

        // TODO BE-06: remove this call — recency update will be atomic in the Lua commit.
        //             Until then this non-atomic SET keeps the gate working end-to-end.
        recencyGate.updateRecency(snapshot)
                .onFailure(err -> log.warn(
                        "event=recency.update_failed topic={} producerId={} error=\"{}\"",
                        snapshot.topic(), snapshot.producerId(), err.getMessage()))
                .onComplete(ar ->
                        // Reply (and release semaphore) AFTER recency is written so
                        // rapid back-to-back snapshots from the same producer cannot
                        // both pass the gate before the first one's recency is stored.
                        msg.reply(new JsonObject().put("status", "ok"))
                );
    }
}
