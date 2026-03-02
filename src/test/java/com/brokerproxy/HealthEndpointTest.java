package com.brokerproxy;

import com.brokerproxy.verticle.HttpServerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@code GET /health} and {@code GET /metrics} endpoints.
 *
 * <p>Lifecycle: one Vertx instance + one deployed {@link HttpServerVerticle} shared
 * across all test methods (cheaper and sufficient since the verticle is stateless).
 *
 * <p>Vertx is initialised with {@link MicrometerMetricsOptions} enabled so that
 * {@code PrometheusScrapingHandler} finds a Prometheus registry — mirroring production.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HealthEndpointTest {

    private static final int    TEST_PORT = 18_080;
    private static final String TEST_HOST = "127.0.0.1";

    private Vertx     vertx;
    private WebClient client;

    // ---- Lifecycle --------------------------------------------------------------

    @BeforeAll
    void setupAll() throws Exception {
        // Initialise with Prometheus metrics — same as Main.java in production.
        MicrometerMetricsOptions metricsOpts = new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setEnabled(true);

        vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(metricsOpts));

        JsonObject config = new JsonObject()
                .put("http", new JsonObject()
                        .put("port", TEST_PORT)
                        .put("host", TEST_HOST));

        // Deploy once; block until ready (test setup — blocking is intentional here).
        vertx.deployVerticle(new HttpServerVerticle(), new DeploymentOptions().setConfig(config))
                .toCompletionStage()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);

        client = WebClient.create(vertx);
    }

    @AfterAll
    void tearDownAll() throws Exception {
        if (client != null) client.close();
        if (vertx  != null) {
            vertx.close()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    // ---- /health tests ----------------------------------------------------------

    @Test
    @DisplayName("GET /health returns 200")
    void health_returns_200() throws Exception {
        var resp = await(client.get(TEST_PORT, TEST_HOST, "/health").send());
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    @Test
    @DisplayName("GET /health returns Content-Type: application/json")
    void health_returns_json_content_type() throws Exception {
        var resp = await(client.get(TEST_PORT, TEST_HOST, "/health").send());
        assertThat(resp.getHeader("Content-Type")).contains("application/json");
    }

    @Test
    @DisplayName("GET /health body contains required fields: status, service, version, build")
    void health_body_contains_required_fields() throws Exception {
        var resp = await(client.get(TEST_PORT, TEST_HOST, "/health").send());
        JsonObject body = resp.bodyAsJsonObject();

        assertThat(body.getString("status")).isEqualTo("ok");
        assertThat(body.getString("service")).isNotBlank();
        assertThat(body.getString("version")).isNotBlank();
        assertThat(body.getString("build")).isNotBlank();
    }

    @Test
    @DisplayName("GET /health body is valid JSON (parseable)")
    void health_body_is_valid_json() throws Exception {
        var resp = await(client.get(TEST_PORT, TEST_HOST, "/health").send());
        // bodyAsJsonObject() throws if body isn't valid JSON
        assertThat(resp.bodyAsJsonObject()).isNotNull();
    }

    // ---- /metrics tests ---------------------------------------------------------

    @Test
    @DisplayName("GET /metrics returns 200 (Prometheus scrape endpoint active)")
    void metrics_endpoint_returns_200() throws Exception {
        var resp = await(client.get(TEST_PORT, TEST_HOST, "/metrics").send());
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    // ---- Helpers ----------------------------------------------------------------

    /** Blocks on a Vert.x {@link Future} for up to 5 s — test-only helper. */
    private <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage()
                .toCompletableFuture()
                .get(5, TimeUnit.SECONDS);
    }
}
