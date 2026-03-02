package com.brokerproxy.verticle;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.model.CommitResult;
import com.brokerproxy.model.RecencyResult;
import com.brokerproxy.model.Snapshot;
import com.brokerproxy.model.WritePlan;
import com.brokerproxy.redis.RedisProvider;
import com.brokerproxy.service.CommitExecutor;
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
 * <h3>Current pipeline (BE-07)</h3>
 * <ol>
 *   <li>Receive snapshot JSON from {@code bp.snapshot.{topic}}</li>
 *   <li>Recency gate — early drop if {@code msgTs <= lastAccepted}</li>
 *   <li>Diff engine — compute {@link WritePlan} (per-topic)</li>
 *   <li>Atomic commit via {@link CommitExecutor} (EVALSHA + retry + metrics)</li>
 *   <li>Reply to release the {@link JmsConsumerVerticle} backpressure semaphore</li>
 * </ol>
 */
public class SnapshotProcessorVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(SnapshotProcessorVerticle.class);

    private static final int DEFAULT_MAX_RETRIES   = 2;
    private static final int DEFAULT_RETRY_DELAY   = 200;   // ms between retries

    // ---- Instance state (safe: one instance per deployment) ----------------------

    private RecencyGate             recencyGate;
    private ComputersDiffEngine     computersDiffEngine;
    private ProducerCacheDiffEngine multiItemDiffEngine;
    private CommitExecutor          commitExecutor;

    // ---- Lifecycle ---------------------------------------------------------------

    @Override
    public void start(Promise<Void> startPromise) {
        AppConfig config   = AppConfig.from(context.config());
        RedisAPI  redisApi = RedisProvider.createApi(vertx, config);

        recencyGate         = new RecencyGate(redisApi, config.redisPrefix());
        computersDiffEngine = new ComputersDiffEngine(redisApi, config.redisPrefix());
        multiItemDiffEngine = new ProducerCacheDiffEngine(redisApi, config.redisPrefix());

        LuaCommitScript script = new LuaCommitScript(redisApi, config.redisPrefix());
        commitExecutor = new CommitExecutor(vertx, script, config.leaderEpoch(),
                DEFAULT_MAX_RETRIES, DEFAULT_RETRY_DELAY);

        // Register event bus consumers BEFORE loading the script so that
        // no messages are lost during the SCRIPT LOAD round-trip.
        for (String topicName : config.topics()) {
            vertx.eventBus().<JsonObject>consumer(
                    JmsConsumerVerticle.SNAPSHOT_ADDR + topicName,
                    this::processSnapshot
            );
        }

        // Load the Lua script into Redis cache; complete start only on success.
        // Any commit before the SHA is cached will fall back to EVAL automatically.
        script.loadScript()
                .onSuccess(v -> {
                    log.info("event=snapshot_processor.started "
                            + "verticle=SnapshotProcessorVerticle topics={}",
                            config.topics());
                    startPromise.complete();
                })
                .onFailure(err -> {
                    log.error("event=snapshot_processor.start_failed error=\"{}\"",
                            err.getMessage());
                    startPromise.fail(err);
                });
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

    private Future<WritePlan> runDiff(Snapshot snapshot) {
        return switch (snapshot.topic()) {
            case "computers"   -> computersDiffEngine.diff(snapshot);
            case "headsets",
                 "conferences" -> multiItemDiffEngine.diff(snapshot);
            default            -> Future.succeededFuture(
                    WritePlan.noop(snapshot.topic(), snapshot.producerId(), snapshot.msgTs()));
        };
    }

    private void commitPlan(Message<JsonObject> msg, Snapshot snapshot, WritePlan plan) {
        commitExecutor.commit(plan)
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
                log.warn("event=commit.fenced topic={} producerId={} msgTs={}",
                        snapshot.topic(), snapshot.producerId(), snapshot.msgTs());
                BrokerMetrics.fencingDenied();
                BrokerMetrics.snapshotDropped(snapshot.topic(), BrokerMetrics.REASON_FENCED);
                msg.reply(new JsonObject()
                        .put("status", "dropped")
                        .put("reason", BrokerMetrics.REASON_FENCED));
            }
            case DROPPED_RECENCY -> {
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
