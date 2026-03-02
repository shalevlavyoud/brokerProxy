package com.brokerproxy.verticle;

import com.brokerproxy.config.AppConfig;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Root verticle for the Broker Proxy service.
 *
 * <p>Responsibilities:
 * <ol>
 *   <li>Load hierarchical configuration: optional {@code config.json} file
 *       (working-directory relative) merged with environment variable overrides.</li>
 *   <li>Deploy sub-verticles ({@link HttpServerVerticle}) with the merged config.</li>
 * </ol>
 *
 * <p>Config-store priority (last wins):
 * <pre>
 *   1. config.json  (base / defaults)
 *   2. Environment variables  (runtime overrides — e.g. HTTP_PORT)
 * </pre>
 */
public class MainVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) {
        loadConfig()
                .compose(this::deploySubVerticles)
                .onSuccess(v -> {
                    log.info("event=main_verticle.started verticle=MainVerticle");
                    startPromise.complete();
                })
                .onFailure(err -> {
                    log.error("event=main_verticle.start_failed verticle=MainVerticle error=\"{}\"",
                            err.getMessage(), err);
                    startPromise.fail(err);
                });
    }

    // ---- Config loading ----------------------------------------------------------

    private Future<JsonObject> loadConfig() {
        // File store: config.json next to the working directory (optional — missing file
        // is silently skipped; all values fall back to AppConfig defaults).
        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setFormat("json")
                .setConfig(new JsonObject().put("path", "config.json"))
                .setOptional(true);

        // Env store: environment variables override file values (e.g. HTTP_PORT=9090).
        ConfigStoreOptions envStore = new ConfigStoreOptions()
                .setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(fileStore)
                .addStore(envStore);

        return ConfigRetriever.create(vertx, options)
                .getConfig()
                .onSuccess(cfg -> {
                    AppConfig appConfig = AppConfig.from(cfg);
                    log.info("event=config.loaded verticle=MainVerticle "
                                    + "httpPort={} redisHost={} redisPrefix={} topics={}",
                            appConfig.httpPort(), appConfig.redisHost(),
                            appConfig.redisPrefix(), appConfig.topics());
                });
    }

    // ---- Sub-verticle deployment -------------------------------------------------

    private Future<Void> deploySubVerticles(JsonObject config) {
        DeploymentOptions opts = new DeploymentOptions().setConfig(config);

        // SnapshotProcessorVerticle must be registered on the event bus BEFORE
        // JmsConsumerVerticle starts delivering messages — deploy it first.
        return vertx.deployVerticle(new SnapshotProcessorVerticle(), opts)
                .compose(id -> vertx.deployVerticle(new JmsConsumerVerticle(), opts))
                .compose(id -> vertx.deployVerticle(new HttpServerVerticle(), opts))
                .mapEmpty();
    }
}
