package com.brokerproxy.config;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Immutable application configuration sourced from a merged Vert.x config
 * (JSON file + environment variable overrides).
 *
 * <p>All timeouts are in milliseconds. Defaults are intentionally conservative
 * so the service can start with zero external config for local development.
 *
 * <p>Environment variable overrides follow the standard Vert.x config-env pattern
 * (e.g. {@code HTTP_PORT=9090}).
 */
public record AppConfig(

        // ---- HTTP server ----
        int httpPort,
        String httpHost,

        // ---- Redis ----
        String redisHost,
        int redisPort,
        String redisPrefix,
        int redisConnectTimeoutMs,
        int commitTimeoutMs,
        /**
         * The epoch this instance believes it is the leader for.
         * The Lua commit script checks this against {@code bp:leader:epoch} stored
         * in Redis; a mismatch returns {@code FENCED} and writes nothing.
         * Default 1 — for local development, seed Redis with {@code SET bp:leader:epoch 1}.
         */
        long leaderEpoch,

        // ---- Topics ----
        List<String> topics,

        // ---- Retention ----
        int retentionWindowSize,

        // ---- ActiveMQ ----
        String activemqBrokerUrl,

        // ---- JMS backpressure ----
        /** ActiveMQ prefetch window per topic consumer (broker-side flow control). */
        int jmsTopicPrefetch,
        /**
         * Max in-flight snapshots per topic before the JMS listener starts dropping.
         * A snapshot counts as in-flight from the moment it is parsed until the
         * SnapshotProcessorVerticle replies via the event bus.
         */
        int jmsMaxPendingPerTopic,
        /** Event-bus request timeout waiting for the processor to reply (ms). */
        int jmsProcessingTimeoutMs,

        // ---- Guardrails ----
        int maxDataJsonBytes,
        int maxChangesPerCommit

) {

    // ---- Defaults ----------------------------------------------------------------

    public static final int     DEFAULT_HTTP_PORT               = 8080;
    public static final String  DEFAULT_HTTP_HOST               = "0.0.0.0";

    public static final String  DEFAULT_REDIS_HOST              = "localhost";
    public static final int     DEFAULT_REDIS_PORT              = 6379;
    public static final String  DEFAULT_REDIS_PREFIX            = "bp";
    public static final int     DEFAULT_REDIS_CONNECT_TIMEOUT   = 5_000;
    public static final int     DEFAULT_COMMIT_TIMEOUT          = 5_000;
    public static final long    DEFAULT_LEADER_EPOCH            = 1L;

    public static final List<String> DEFAULT_TOPICS =
            List.of("computers", "headsets", "conferences");

    public static final int     DEFAULT_RETENTION_WINDOW        = 10_000;
    public static final String  DEFAULT_ACTIVEMQ_URL            = "tcp://localhost:61616";

    public static final int     DEFAULT_JMS_TOPIC_PREFETCH      = 10;
    public static final int     DEFAULT_JMS_MAX_PENDING         = 100;
    public static final int     DEFAULT_JMS_PROCESSING_TIMEOUT  = 5_000;

    public static final int     DEFAULT_MAX_DATA_JSON_BYTES     = 65_536;   // 64 KiB
    public static final int     DEFAULT_MAX_CHANGES_PER_COMMIT  = 1_000;

    // ---- Factory -----------------------------------------------------------------

    /**
     * Builds an {@link AppConfig} from the merged Vert.x config {@link JsonObject}.
     * Missing sections / keys fall back to the defaults above.
     */
    public static AppConfig from(JsonObject json) {
        if (json == null) json = new JsonObject();

        JsonObject http      = json.getJsonObject("http",       new JsonObject());
        JsonObject redis     = json.getJsonObject("redis",      new JsonObject());
        JsonObject amq       = json.getJsonObject("activemq",   new JsonObject());
        JsonObject jms       = json.getJsonObject("jms",        new JsonObject());
        JsonObject retention = json.getJsonObject("retention",  new JsonObject());
        JsonObject guard     = json.getJsonObject("guardrails", new JsonObject());

        JsonArray topicsArr  = json.getJsonArray("topics",
                new JsonArray().add("computers").add("headsets").add("conferences"));

        List<String> topics = topicsArr.stream()
                .map(Object::toString)
                .collect(Collectors.toUnmodifiableList());

        return new AppConfig(
                http.getInteger("port",            DEFAULT_HTTP_PORT),
                http.getString ("host",            DEFAULT_HTTP_HOST),
                redis.getString("host",            DEFAULT_REDIS_HOST),
                redis.getInteger("port",           DEFAULT_REDIS_PORT),
                redis.getString("prefix",          DEFAULT_REDIS_PREFIX),
                redis.getInteger("connectTimeout", DEFAULT_REDIS_CONNECT_TIMEOUT),
                redis.getInteger("commitTimeout",  DEFAULT_COMMIT_TIMEOUT),
                redis.getLong   ("leaderEpoch",    DEFAULT_LEADER_EPOCH),
                topics,
                retention.getInteger("windowSize",        DEFAULT_RETENTION_WINDOW),
                amq.getString("brokerUrl",                DEFAULT_ACTIVEMQ_URL),
                jms.getInteger("topicPrefetch",           DEFAULT_JMS_TOPIC_PREFETCH),
                jms.getInteger("maxPendingPerTopic",      DEFAULT_JMS_MAX_PENDING),
                jms.getInteger("processingTimeoutMs",     DEFAULT_JMS_PROCESSING_TIMEOUT),
                guard.getInteger("maxDataJsonBytes",      DEFAULT_MAX_DATA_JSON_BYTES),
                guard.getInteger("maxChangesPerCommit",   DEFAULT_MAX_CHANGES_PER_COMMIT)
        );
    }
}
