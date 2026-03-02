package com.brokerproxy;

import com.brokerproxy.verticle.MainVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the Broker Proxy service.
 *
 * <p>Bootstraps a Vert.x instance with Prometheus metrics enabled, then deploys
 * {@link MainVerticle} which loads configuration and sets up all sub-verticles.
 */
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        MicrometerMetricsOptions metricsOptions = new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setEnabled(true);

        Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOptions));

        vertx.deployVerticle(new MainVerticle())
                .onSuccess(id -> log.info(
                        "event=startup.complete service=broker-proxy deploymentId={}", id))
                .onFailure(err -> {
                    log.error(
                            "event=startup.failed service=broker-proxy error=\"{}\"",
                            err.getMessage(), err);
                    vertx.close().onComplete(v -> System.exit(1));
                });
    }
}
