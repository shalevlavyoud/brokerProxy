package com.brokerproxy.verticle;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.model.Snapshot;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event-loop verticle that receives parsed {@link Snapshot} objects from the
 * event bus and drives the processing pipeline.
 *
 * <p>Current pipeline (BE-02):
 * <ol>
 *   <li>Receive snapshot JSON from {@code bp.snapshot.{topic}}</li>
 *   <li>Log structured event</li>
 *   <li>Reply to release the {@link JmsConsumerVerticle} backpressure semaphore</li>
 * </ol>
 *
 * <p>Pipeline will be extended in subsequent tasks:
 * <ul>
 *   <li>BE-03 — recency gate (drop stale msgTs)</li>
 *   <li>BE-04 — computers diff engine</li>
 *   <li>BE-05 — headsets / conferences diff engine</li>
 *   <li>BE-06 — atomic Lua commit to Redis</li>
 * </ul>
 */
public class SnapshotProcessorVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(SnapshotProcessorVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) {
        AppConfig config = AppConfig.from(context.config());

        for (String topicName : config.topics()) {
            vertx.eventBus().<JsonObject>consumer(
                    JmsConsumerVerticle.SNAPSHOT_ADDR + topicName,
                    msg -> {
                        Snapshot snapshot = Snapshot.fromJson(msg.body());

                        log.info("event=snapshot.received topic={} producerId={} "
                                        + "msgTs={} itemCount={}",
                                snapshot.topic(), snapshot.producerId(),
                                snapshot.msgTs(), snapshot.items().size());

                        // Reply immediately — releases the JmsConsumerVerticle semaphore
                        // (BE-03+ will reply after pipeline stages complete)
                        msg.reply(new JsonObject().put("status", "ok"));
                    }
            );
        }

        log.info("event=snapshot_processor.started verticle=SnapshotProcessorVerticle "
                + "topics={}", config.topics());
        startPromise.complete();
    }
}
