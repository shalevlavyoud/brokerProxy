package com.brokerproxy.handler;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.metrics.BrokerMetrics;
import com.brokerproxy.service.ChangesService;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Handles {@code GET /changes?topic=&sinceSeq=&limit=}.
 *
 * <h3>Request parameters</h3>
 * <ul>
 *   <li>{@code topic}    — required; 400 if absent or not in configured topics</li>
 *   <li>{@code sinceSeq} — required long ≥ 0; 400 if absent or non-numeric</li>
 *   <li>{@code limit}    — optional int; defaults to {@link AppConfig#changesDefaultLimit()};
 *                          silently capped at {@link AppConfig#changesMaxLimit()} (not a 400)</li>
 * </ul>
 *
 * <h3>Response (200)</h3>
 * <pre>
 * {
 *   "items":   [{ "itemId": "x", "version": 3, "dataJson": "{…}" }],
 *   "deleted": ["y"],
 *   "newSeq":  42,
 *   "fullResyncRequired": false
 * }
 * </pre>
 *
 * <h3>Error body (4xx / 5xx)</h3>
 * <pre>
 * { "error": "MACHINE_CODE", "message": "human-readable", "correlationId": "…" }
 * </pre>
 *
 * <h3>Observability</h3>
 * <ul>
 *   <li>{@code X-Correlation-Id} header propagated from request (or generated if absent)</li>
 *   <li>Prometheus metrics emitted via {@link BrokerMetrics}</li>
 *   <li>Query timeout configurable via {@link AppConfig#changesQueryTimeoutMs()}</li>
 * </ul>
 */
public class ChangesHandler implements Handler<RoutingContext> {

    private static final Logger log = LoggerFactory.getLogger(ChangesHandler.class);

    private static final String CONTENT_TYPE       = "application/json";
    private static final String CORRELATION_HEADER = "X-Correlation-Id";

    private final ChangesService service;
    private final AppConfig      config;

    public ChangesHandler(ChangesService service, AppConfig config) {
        this.service = service;
        this.config  = config;
    }

    @Override
    public void handle(RoutingContext ctx) {
        long   start         = System.currentTimeMillis();
        String correlationId = resolveCorrelationId(ctx);

        // ---- Parameter validation ------------------------------------------------

        String topic = ctx.request().getParam("topic");
        if (topic == null || topic.isBlank()) {
            sendError(ctx, 400, "MISSING_PARAM", "topic is required", correlationId);
            return;
        }
        if (!config.topics().contains(topic)) {
            sendError(ctx, 400, "UNKNOWN_TOPIC",
                    "topic '" + topic + "' is not configured", correlationId);
            return;
        }

        String sinceSeqStr = ctx.request().getParam("sinceSeq");
        if (sinceSeqStr == null || sinceSeqStr.isBlank()) {
            sendError(ctx, 400, "MISSING_PARAM", "sinceSeq is required", correlationId);
            return;
        }
        long sinceSeq;
        try {
            sinceSeq = Long.parseLong(sinceSeqStr);
            if (sinceSeq < 0) {
                sendError(ctx, 400, "INVALID_PARAM", "sinceSeq must be >= 0", correlationId);
                return;
            }
        } catch (NumberFormatException e) {
            sendError(ctx, 400, "INVALID_PARAM",
                    "sinceSeq must be a non-negative integer", correlationId);
            return;
        }

        int limit = config.changesDefaultLimit();
        String limitStr = ctx.request().getParam("limit");
        if (limitStr != null && !limitStr.isBlank()) {
            try {
                limit = Integer.parseInt(limitStr);
                if (limit <= 0) {
                    sendError(ctx, 400, "INVALID_PARAM",
                            "limit must be a positive integer", correlationId);
                    return;
                }
            } catch (NumberFormatException e) {
                sendError(ctx, 400, "INVALID_PARAM", "limit must be an integer", correlationId);
                return;
            }
        }
        // Silently cap — never 400 for an over-limit request
        limit = Math.min(limit, config.changesMaxLimit());

        // ---- Query with configurable timeout ------------------------------------

        final String finalTopic = topic;
        final long   finalSince = sinceSeq;
        final int    finalLimit = limit;

        withTimeout(ctx.vertx(), service.query(finalTopic, finalSince, finalLimit), correlationId)
                .onSuccess(resp -> {
                    long durationMs = System.currentTimeMillis() - start;
                    BrokerMetrics.changesRequest(finalTopic, "success");
                    BrokerMetrics.changesItemsReturned(finalTopic, resp.items().size());
                    BrokerMetrics.recordChangesDuration(finalTopic, durationMs);

                    log.info("event=changes.ok correlationId={} topic={} sinceSeq={} "
                                    + "items={} deleted={} newSeq={} fullResync={} durationMs={}",
                            correlationId, finalTopic, finalSince,
                            resp.items().size(), resp.deleted().size(),
                            resp.newSeq(), resp.fullResyncRequired(), durationMs);

                    ctx.response()
                            .setStatusCode(200)
                            .putHeader("Content-Type", CONTENT_TYPE)
                            .putHeader(CORRELATION_HEADER, correlationId)
                            .end(serializeResponse(resp).encode());
                })
                .onFailure(err -> {
                    long durationMs = System.currentTimeMillis() - start;
                    BrokerMetrics.changesRequest(finalTopic, "error");
                    BrokerMetrics.recordChangesDuration(finalTopic, durationMs);

                    log.warn("event=changes.error correlationId={} topic={} sinceSeq={} "
                                    + "durationMs={} error=\"{}\"",
                            correlationId, finalTopic, finalSince, durationMs, err.getMessage());

                    sendError(ctx, 503, "SERVICE_UNAVAILABLE",
                            "Changes query failed: " + err.getMessage(), correlationId);
                });
    }

    // ---- Timeout wrapper -------------------------------------------------------

    /**
     * Wraps the service {@link io.vertx.core.Future} with a per-request timeout.
     *
     * <p>Uses the same event-loop-safe timer pattern as {@code CommitExecutor.withTimeout}:
     * both the timer callback and the future callback run on the same event-loop thread,
     * so no synchronisation is needed — only one branch will win the
     * {@code !promise.future().isComplete()} guard.
     */
    private <T> io.vertx.core.Future<T> withTimeout(Vertx vertx,
                                                      io.vertx.core.Future<T> f,
                                                      String correlationId) {
        int timeoutMs = config.changesQueryTimeoutMs();
        if (timeoutMs <= 0) return f;

        Promise<T> promise = Promise.promise();
        long timerId = vertx.setTimer(timeoutMs, id -> {
            if (!promise.future().isComplete()) {
                log.warn("event=changes.timeout correlationId={} changesQueryTimeoutMs={}",
                        correlationId, timeoutMs);
                promise.fail(new TimeoutException(
                        "Changes query timed out after " + timeoutMs + "ms"));
            }
        });
        f.onComplete(ar -> {
            vertx.cancelTimer(timerId);
            if (!promise.future().isComplete()) {
                promise.handle(ar);
            }
        });
        return promise.future();
    }

    // ---- Response serialization -------------------------------------------------

    private static JsonObject serializeResponse(ChangesService.ChangesResponse resp) {
        JsonArray itemsArr = new JsonArray();
        for (ChangesService.ChangesItem item : resp.items()) {
            itemsArr.add(new JsonObject()
                    .put("itemId",   item.itemId())
                    .put("version",  item.version())
                    .put("dataJson", item.dataJson()));
        }
        return new JsonObject()
                .put("items",              itemsArr)
                .put("deleted",            new JsonArray(resp.deleted()))
                .put("newSeq",             resp.newSeq())
                .put("fullResyncRequired", resp.fullResyncRequired());
    }

    // ---- Error helpers ----------------------------------------------------------

    private static void sendError(RoutingContext ctx, int status, String error,
                                   String message, String correlationId) {
        JsonObject body = new JsonObject()
                .put("error",         error)
                .put("message",       message)
                .put("correlationId", correlationId);
        ctx.response()
                .setStatusCode(status)
                .putHeader("Content-Type", CONTENT_TYPE)
                .putHeader(CORRELATION_HEADER, correlationId)
                .end(body.encode());
    }

    /** Extracts {@code X-Correlation-Id} from the request header, generating a UUID if absent. */
    private static String resolveCorrelationId(RoutingContext ctx) {
        String id = ctx.request().getHeader(CORRELATION_HEADER);
        return (id != null && !id.isBlank()) ? id : UUID.randomUUID().toString();
    }
}
