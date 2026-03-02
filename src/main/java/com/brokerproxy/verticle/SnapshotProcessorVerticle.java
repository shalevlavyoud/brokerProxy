package com.brokerproxy.verticle;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.model.RecencyResult;
import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.WritePlan;
import com.brokerproxy.redis.RedisProvider;
import com.brokerproxy.model.CommitResult;
import com.brokerproxy.service.ComputersDiffEngine;
import com.brokerproxy.service.LuaCommitScript;
import com.brokerproxy.service.ProducerCacheDiffEngine;
import com.brokerproxy.service.RecencyGate;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
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
 * <h3>Current pipeline (BE-05)</h3>
 * <ol>
 *   <li>Receive snapshot JSON from {@code bp.snapshot.{topic}}</li>
 *   <li>Recency gate — drop if {@code msgTs <= lastAccepted} stored in Redis</li>
 *   <li>Diff engine — compute {@link WritePlan} for all three topics</li>
 *   <li>Log plan size + metrics</li>
 *   <li>Reply to release the {@link JmsConsumerVerticle} backpressure semaphore</li>
 * </ol>
 *
 * <h3>Future pipeline extensions</h3>
 * <ul>
 *   <li>BE-06 — atomic Lua commit (recency write moved there, updateRecency() removed)</li>
 * </ul>
 */
public class SnapshotProcessorVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(SnapshotProcessorVerticle.class);

    // ---- Instance state (safe: one instance per deployment) ----------------------

    private RecencyGate             recencyGate;
    private ComputersDiffEngine     computersDiffEngine;
    private ProducerCacheDiffEngine multiItemDiffEngine;   // headsets + conferences
    private LuaCommitScript         commitScript;
    private long                    leaderEpoch;

    // ---- Lifecycle ---------------------------------------------------------------

    @Override
    public void start(Promise<Void> startPromise) {
        AppConfig config   = AppConfig.from(context.config());
        RedisAPI  redisApi = RedisProvider.createApi(vertx, config);

        recencyGate         = new RecencyGate(redisApi, config.redisPrefix());
        computersDiffEngine = new ComputersDiffEngine(redisApi, config.redisPrefix());
        multiItemDiffEngine = new ProducerCacheDiffEngine(redisApi, config.redisPrefix());
        commitScript        = new LuaCommitScript(redisApi, config.redisPrefix());
        leaderEpoch         = config.leaderEpoch();

        for (String topicName : config.topics()) {
            vertx.eventBus().<JsonObject>consumer(
                    JmsConsumerVerticle.SNAPSHOT_ADDR + topicName,
                    this::processSnapshot
            );
        }

        log.info("event=snapshot_processor.started verticle=SnapshotProcessorVerticle "
                + "topics={}", config.topics());
        startPromise.complete();
    }

    // ---- Processing pipeline ----------------------------------------------------

    private void processSnapshot(Message<JsonObject> msg) {
        Snapshot snapshot = Snapshot.fromJson(msg.body());

        recencyGate.check(snapshot)
                .onSuccess(result -> handleRecencyResult(msg, snapshot, result))
                .onFailure(err -> {
                    log.error("event=recency.check_failed topic={} producerId={} "
                                    + "msgTs={} error=\"{}\"",
                            snapshot.topic(), snapshot.producerId(),
                            snapshot.msgTs(), err.getMessage(), err);
                    BrokerMetrics.snapshotDropped(snapshot.topic(),
                            BrokerMetrics.REASON_REDIS_ERROR);
                    msg.reply(new JsonObject()
                            .put("status", "dropped")
                            .put("reason", BrokerMetrics.REASON_REDIS_ERROR));
                });
    }

    private void handleRecencyResult(Message<JsonObject> msg, Snapshot snapshot,
                                     RecencyResult result) {
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

        // ---- Accept: log + diff -------------------------------------------------
        log.info("event=snapshot.accepted topic={} producerId={} msgTs={} itemCount={}",
                snapshot.topic(), snapshot.producerId(),
                snapshot.msgTs(), snapshot.items().size());

        BrokerMetrics.snapshotReceived(snapshot.topic());

        runDiff(snapshot)
                .onSuccess(plan -> commitPlan(msg, snapshot, plan))
                .onFailure(err -> {
                    log.error("event=diff.failed topic={} producerId={} msgTs={} error=\"{}\"",
                            snapshot.topic(), snapshot.producerId(),
                            snapshot.msgTs(), err.getMessage(), err);
                    BrokerMetrics.snapshotDropped(snapshot.topic(),
                            BrokerMetrics.REASON_REDIS_ERROR);
                    msg.reply(new JsonObject()
                            .put("status", "dropped")
                            .put("reason", BrokerMetrics.REASON_REDIS_ERROR));
                });
    }

    /**
     * Dispatches to the correct diff engine for the snapshot's topic.
     * <ul>
     *   <li>{@code computers} — {@link ComputersDiffEngine} (single-item producer)</li>
     *   <li>{@code headsets}, {@code conferences} — {@link ProducerCacheDiffEngine}
     *       (multi-item producer)</li>
     * </ul>
     */
    private Future<WritePlan> runDiff(Snapshot snapshot) {
        return switch (snapshot.topic()) {
            case "computers"    -> computersDiffEngine.diff(snapshot);
            case "headsets",
                 "conferences"  -> multiItemDiffEngine.diff(snapshot);
            default             -> Future.succeededFuture(
                    WritePlan.noop(snapshot.topic(), snapshot.producerId(), snapshot.msgTs()));
        };
    }

    /**
     * Executes the atomic Lua commit and replies to release the JMS semaphore.
     *
     * <p>The Lua script atomically:
     * <ol>
     *   <li>Verifies the leader epoch (fencing)</li>
     *   <li>Verifies msgTs recency</li>
     *   <li>Allocates SEQs and writes state, change-indices, prodver cache</li>
     *   <li>Updates the recency key</li>
     * </ol>
     */
    private void commitPlan(Message<JsonObject> msg, Snapshot snapshot, WritePlan plan) {
        commitScript.eval(plan, leaderEpoch)
                .onSuccess(result -> handleCommitResult(msg, snapshot, result))
                .onFailure(err -> {
                    log.error("event=commit.error topic={} producerId={} msgTs={} error=\"{}\"",
                            snapshot.topic(), snapshot.producerId(),
                            snapshot.msgTs(), err.getMessage(), err);
                    BrokerMetrics.snapshotDropped(snapshot.topic(),
                            BrokerMetrics.REASON_REDIS_ERROR);
                    msg.reply(new JsonObject()
                            .put("status", "dropped")
                            .put("reason", BrokerMetrics.REASON_REDIS_ERROR));
                });
    }

    private void handleCommitResult(Message<JsonObject> msg, Snapshot snapshot,
                                    CommitResult result) {
        switch (result.status()) {
            case OK -> {
                log.info("event=commit.ok topic={} producerId={} msgTs={} "
                                + "firstSeq={} lastSeq={} upserts={} deletes={}",
                        snapshot.topic(), snapshot.producerId(), snapshot.msgTs(),
                        result.firstSeq(), result.lastSeq(),
                        result.appliedUpserts(), result.appliedDeletes());
                msg.reply(new JsonObject()
                        .put("status",   "ok")
                        .put("firstSeq", result.firstSeq())
                        .put("lastSeq",  result.lastSeq())
                        .put("upserts",  result.appliedUpserts())
                        .put("deletes",  result.appliedDeletes()));
            }
            case FENCED -> {
                log.warn("event=commit.fenced topic={} producerId={} msgTs={} "
                                + "leaderEpoch={}",
                        snapshot.topic(), snapshot.producerId(),
                        snapshot.msgTs(), leaderEpoch);
                BrokerMetrics.fencingDenied();
                BrokerMetrics.snapshotDropped(snapshot.topic(), BrokerMetrics.REASON_FENCED);
                msg.reply(new JsonObject()
                        .put("status", "dropped")
                        .put("reason", BrokerMetrics.REASON_FENCED));
            }
            case DROPPED_RECENCY -> {
                // Late recency detection (Lua saw a race the Java check missed)
                log.info("event=commit.dropped_recency topic={} producerId={} msgTs={}",
                        snapshot.topic(), snapshot.producerId(), snapshot.msgTs());
                BrokerMetrics.snapshotDropped(snapshot.topic(), BrokerMetrics.REASON_RECENCY);
                msg.reply(new JsonObject()
                        .put("status", "dropped")
                        .put("reason", BrokerMetrics.REASON_RECENCY));
            }
        }
    }
}
