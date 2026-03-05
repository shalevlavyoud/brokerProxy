package com.brokerproxy.verticle;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.handler.ChangesHandler;
import com.brokerproxy.handler.HealthHandler;
import com.brokerproxy.redis.RedisProvider;
import com.brokerproxy.service.ChangesService;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.micrometer.PrometheusScrapingHandler;
import io.vertx.redis.client.RedisAPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Vert.x HTTP server verticle.
 *
 * <p>Manages the lifecycle of the HTTP server and owns the Vert.x Web {@link Router}.
 * All route registrations live here; handler classes remain stateless and focused.
 *
 * <p>Routes:
 * <ul>
 *   <li>{@code GET /health}   -- liveness probe; always 200 if JVM is alive</li>
 *   <li>{@code GET /metrics}  -- Prometheus scrape endpoint</li>
 *   <li>{@code GET /changes}  -- paginated change-index read (BE-08)</li>
 * </ul>
 */
public class HttpServerVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(HttpServerVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) {
        AppConfig      config    = AppConfig.from(context.config());
        RedisAPI       redisApi  = RedisProvider.createApi(vertx, config);
        ChangesService changes   = new ChangesService(redisApi, config.redisPrefix());

        Router router = buildRouter(changes, config);

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(config.httpPort(), config.httpHost())
                .onSuccess(server -> {
                    log.info("event=http_server.started verticle=HttpServerVerticle "
                                    + "host={} port={}",
                            config.httpHost(), server.actualPort());
                    startPromise.complete();
                })
                .onFailure(err -> {
                    log.error("event=http_server.start_failed verticle=HttpServerVerticle "
                                    + "host={} port={} error=\"{}\"",
                            config.httpHost(), config.httpPort(), err.getMessage(), err);
                    startPromise.fail(err);
                });
    }

    // ---- Router setup -----------------------------------------------------------

    private Router buildRouter(ChangesService changesService, AppConfig config) {
        Router router = Router.router(vertx);

        // Parse request bodies (needed for future POST/PUT routes)
        router.route().handler(BodyHandler.create());

        // Liveness probe -- intentionally lightweight; no I/O
        router.get("/health").handler(new HealthHandler());

        // Prometheus metrics scrape
        router.get("/metrics").handler(PrometheusScrapingHandler.create());

        // Paginated change-index read (BE-08)
        router.get("/changes").handler(new ChangesHandler(changesService, config));

        return router;
    }
}
