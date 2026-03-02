package com.brokerproxy.handler;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Handles {@code GET /health}.
 *
 * <p>Returns a lightweight liveness response — no Redis or downstream probes here.
 * A dedicated readiness probe (BE-future) will verify Redis connectivity.
 *
 * <p>Response body:
 * <pre>
 * {
 *   "status":  "ok",
 *   "service": "broker-proxy",
 *   "version": "1.0.0-SNAPSHOT",
 *   "build":   "2024-01-01T00:00:00Z"
 * }
 * </pre>
 *
 * <p>Version metadata is read from {@code /version.properties} which is populated
 * by Maven resource filtering at build time.
 */
public class HealthHandler implements Handler<RoutingContext> {

    private static final Logger log = LoggerFactory.getLogger(HealthHandler.class);

    private static final String CONTENT_TYPE = "application/json";

    private final String version;
    private final String build;
    private final String service;

    public HealthHandler() {
        Properties props = loadVersionProperties();
        this.version = props.getProperty("app.version",   "unknown");
        this.build   = props.getProperty("app.build",     "unknown");
        this.service = props.getProperty("app.service",   "broker-proxy");
    }

    @Override
    public void handle(RoutingContext ctx) {
        JsonObject body = new JsonObject()
                .put("status",  "ok")
                .put("service", service)
                .put("version", version)
                .put("build",   build);

        ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", CONTENT_TYPE)
                .end(body.encode());
    }

    // ---- Helpers ----------------------------------------------------------------

    private static Properties loadVersionProperties() {
        Properties props = new Properties();
        try (InputStream is = HealthHandler.class.getResourceAsStream("/version.properties")) {
            if (is != null) {
                props.load(is);
            } else {
                log.warn("event=health_handler.version_file_missing "
                        + "resource=/version.properties — build info will show 'unknown'");
            }
        } catch (Exception e) {
            log.warn("event=health_handler.version_load_failed error=\"{}\"", e.getMessage());
        }
        return props;
    }
}
