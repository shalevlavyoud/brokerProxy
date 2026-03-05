package com.brokerproxy.handler;

import com.brokerproxy.config.AppConfig;
import com.brokerproxy.service.ChangesService;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ChangesHandler}.
 *
 * <p>Uses a real Vert.x HTTP server on an ephemeral port with a Mockito-mocked
 * {@link ChangesService} — no Redis required.
 *
 * <h3>Covered scenarios</h3>
 * <ol>
 *   <li>Missing {@code topic} param &rarr; 400 MISSING_PARAM</li>
 *   <li>Unknown topic &rarr; 400 UNKNOWN_TOPIC</li>
 *   <li>Missing {@code sinceSeq} param &rarr; 400 MISSING_PARAM</li>
 *   <li>Non-numeric {@code sinceSeq} &rarr; 400 INVALID_PARAM</li>
 *   <li>Negative {@code sinceSeq} &rarr; 400 INVALID_PARAM</li>
 *   <li>{@code limit} above changesMaxLimit is silently clamped (not 400)</li>
 *   <li>fullResyncRequired response &rarr; 200 with correct body shape</li>
 *   <li>Normal result &rarr; 200 with items + deleted + newSeq</li>
 *   <li>Service failure &rarr; 503 with error body</li>
 *   <li>{@code X-Correlation-Id} header propagated to response</li>
 *   <li>Correlation-Id generated when absent</li>
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ChangesHandlerTest {

    private static final String TOPIC = "computers";

    private Vertx          vertx;
    private WebClient      client;
    private ChangesService mockService;
    private int            port;

    // ---- Setup / teardown -------------------------------------------------------

    @BeforeAll
    void setupAll() throws Exception {
        vertx       = Vertx.vertx();
        mockService = mock(ChangesService.class);

        // Use all defaults -- changesDefaultLimit=100, changesMaxLimit=1000
        AppConfig config = AppConfig.from(new JsonObject());

        Router router = Router.router(vertx);
        router.get("/changes").handler(new ChangesHandler(mockService, config));

        CompletableFuture<Integer> portFuture = new CompletableFuture<>();
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(0)
                .onSuccess(s -> portFuture.complete(s.actualPort()))
                .onFailure(portFuture::completeExceptionally);

        port   = portFuture.get(5, TimeUnit.SECONDS);
        client = WebClient.create(vertx, new WebClientOptions().setDefaultPort(port).setDefaultHost("localhost"));
    }

    @AfterAll
    void tearDownAll() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @BeforeEach
    void resetMocks() {
        reset(mockService);
    }

    // ---- 400 validation tests ---------------------------------------------------

    @Test
    @DisplayName("Missing topic param -> 400 MISSING_PARAM")
    void missing_topic_returns_400() throws Exception {
        JsonObject body = get("/changes?sinceSeq=0", 400);

        assertThat(body.getString("error")).isEqualTo("MISSING_PARAM");
        assertThat(body.getString("message")).contains("topic");
    }

    @Test
    @DisplayName("Unknown topic -> 400 UNKNOWN_TOPIC")
    void unknown_topic_returns_400() throws Exception {
        JsonObject body = get("/changes?topic=unknown&sinceSeq=0", 400);

        assertThat(body.getString("error")).isEqualTo("UNKNOWN_TOPIC");
    }

    @Test
    @DisplayName("Missing sinceSeq param -> 400 MISSING_PARAM")
    void missing_sinceSeq_returns_400() throws Exception {
        JsonObject body = get("/changes?topic=" + TOPIC, 400);

        assertThat(body.getString("error")).isEqualTo("MISSING_PARAM");
        assertThat(body.getString("message")).contains("sinceSeq");
    }

    @Test
    @DisplayName("Non-numeric sinceSeq -> 400 INVALID_PARAM")
    void non_numeric_sinceSeq_returns_400() throws Exception {
        JsonObject body = get("/changes?topic=" + TOPIC + "&sinceSeq=abc", 400);

        assertThat(body.getString("error")).isEqualTo("INVALID_PARAM");
    }

    @Test
    @DisplayName("Negative sinceSeq -> 400 INVALID_PARAM")
    void negative_sinceSeq_returns_400() throws Exception {
        JsonObject body = get("/changes?topic=" + TOPIC + "&sinceSeq=-1", 400);

        assertThat(body.getString("error")).isEqualTo("INVALID_PARAM");
        assertThat(body.getString("message")).contains(">= 0");
    }

    // ---- Limit clamping (not 400) -----------------------------------------------

    @Test
    @DisplayName("limit > changesMaxLimit is silently clamped to max, not a 400")
    void overlimit_is_clamped_to_max() throws Exception {
        // Service should be called with limit=1000 (the max), not 99999
        ChangesService.ChangesResponse resp = new ChangesService.ChangesResponse(
                List.of(), List.of(), 0L, false);
        when(mockService.query(eq(TOPIC), eq(0L), eq(1000)))
                .thenReturn(Future.succeededFuture(resp));

        JsonObject body = get("/changes?topic=" + TOPIC + "&sinceSeq=0&limit=99999", 200);

        assertThat(body.getBoolean("fullResyncRequired")).isFalse();
        verify(mockService).query(TOPIC, 0L, 1000);
    }

    // ---- Success responses -------------------------------------------------------

    @Test
    @DisplayName("fullResyncRequired=true -> 200 with empty items/deleted and flag set")
    void fullResync_response() throws Exception {
        ChangesService.ChangesResponse resp = ChangesService.ChangesResponse.fullResync(30L);
        when(mockService.query(eq(TOPIC), eq(30L), anyInt()))
                .thenReturn(Future.succeededFuture(resp));

        JsonObject body = get("/changes?topic=" + TOPIC + "&sinceSeq=30", 200);

        assertThat(body.getBoolean("fullResyncRequired")).isTrue();
        assertThat(body.getJsonArray("items")).isEmpty();
        assertThat(body.getJsonArray("deleted")).isEmpty();
        assertThat(body.getLong("newSeq")).isEqualTo(30L);
    }

    @Test
    @DisplayName("Normal result -> 200 with items + deleted + correct newSeq")
    void normal_result_response() throws Exception {
        List<ChangesService.ChangesItem> items = List.of(
                new ChangesService.ChangesItem("item-1", 3L, "{\"k\":\"v\"}"));
        List<String> deleted = List.of("item-2");
        ChangesService.ChangesResponse resp =
                new ChangesService.ChangesResponse(items, deleted, 42L, false);

        when(mockService.query(eq(TOPIC), eq(0L), anyInt()))
                .thenReturn(Future.succeededFuture(resp));

        JsonObject body = get("/changes?topic=" + TOPIC + "&sinceSeq=0", 200);

        assertThat(body.getBoolean("fullResyncRequired")).isFalse();
        assertThat(body.getLong("newSeq")).isEqualTo(42L);
        assertThat(body.getJsonArray("items")).hasSize(1);
        assertThat(body.getJsonArray("deleted")).hasSize(1);
        assertThat(body.getJsonArray("deleted").getString(0)).isEqualTo("item-2");

        JsonObject item = body.getJsonArray("items").getJsonObject(0);
        assertThat(item.getString("itemId")).isEqualTo("item-1");
        assertThat(item.getLong("version")).isEqualTo(3L);
        assertThat(item.getString("dataJson")).isEqualTo("{\"k\":\"v\"}");
    }

    @Test
    @DisplayName("Default limit is applied when limit param is absent")
    void default_limit_applied_when_absent() throws Exception {
        ChangesService.ChangesResponse resp =
                new ChangesService.ChangesResponse(List.of(), List.of(), 0L, false);
        // changesDefaultLimit default = 100
        when(mockService.query(eq(TOPIC), eq(0L), eq(100)))
                .thenReturn(Future.succeededFuture(resp));

        get("/changes?topic=" + TOPIC + "&sinceSeq=0", 200);

        verify(mockService).query(TOPIC, 0L, 100);
    }

    // ---- Error handling ---------------------------------------------------------

    @Test
    @DisplayName("Service failure -> 503 SERVICE_UNAVAILABLE with error body")
    void service_failure_returns_503() throws Exception {
        when(mockService.query(eq(TOPIC), eq(0L), anyInt()))
                .thenReturn(Future.failedFuture(new RuntimeException("Redis down")));

        JsonObject body = get("/changes?topic=" + TOPIC + "&sinceSeq=0", 503);

        assertThat(body.getString("error")).isEqualTo("SERVICE_UNAVAILABLE");
        assertThat(body.getString("message")).contains("Redis down");
        assertThat(body.getString("correlationId")).isNotBlank();
    }

    // ---- Correlation-Id propagation ---------------------------------------------

    @Test
    @DisplayName("X-Correlation-Id from request is echoed in response header")
    void correlationId_echoed_in_response() throws Exception {
        String correlationId = "test-correlation-123";
        ChangesService.ChangesResponse resp =
                new ChangesService.ChangesResponse(List.of(), List.of(), 0L, false);
        when(mockService.query(eq(TOPIC), eq(0L), anyInt()))
                .thenReturn(Future.succeededFuture(resp));

        io.vertx.ext.web.client.HttpResponse<JsonObject> httpResp =
                client.get("/changes?topic=" + TOPIC + "&sinceSeq=0")
                        .putHeader("X-Correlation-Id", correlationId)
                        .as(BodyCodec.jsonObject())
                        .send()
                        .toCompletionStage().toCompletableFuture()
                        .get(5, TimeUnit.SECONDS);

        assertThat(httpResp.statusCode()).isEqualTo(200);
        assertThat(httpResp.getHeader("X-Correlation-Id")).isEqualTo(correlationId);
        // Body also contains correlationId only in error responses; check only header here
    }

    @Test
    @DisplayName("Correlation-Id generated when X-Correlation-Id header is absent")
    void correlationId_generated_when_absent() throws Exception {
        when(mockService.query(eq(TOPIC), eq(0L), anyInt()))
                .thenReturn(Future.failedFuture(new RuntimeException("boom")));

        io.vertx.ext.web.client.HttpResponse<JsonObject> httpResp =
                client.get("/changes?topic=" + TOPIC + "&sinceSeq=0")
                        .as(BodyCodec.jsonObject())
                        .send()
                        .toCompletionStage().toCompletableFuture()
                        .get(5, TimeUnit.SECONDS);

        assertThat(httpResp.statusCode()).isEqualTo(503);
        // Generated UUID must be non-null and present in both header and body
        String headerCid = httpResp.getHeader("X-Correlation-Id");
        String bodyCid   = httpResp.body().getString("correlationId");
        assertThat(headerCid).isNotBlank();
        assertThat(bodyCid).isEqualTo(headerCid);
    }

    // ---- Helpers ----------------------------------------------------------------

    /** Sends a GET request, asserts status code, and returns the parsed JSON body. */
    private JsonObject get(String path, int expectedStatus) throws Exception {
        io.vertx.ext.web.client.HttpResponse<JsonObject> resp =
                client.get(path)
                        .as(BodyCodec.jsonObject())
                        .send()
                        .toCompletionStage().toCompletableFuture()
                        .get(5, TimeUnit.SECONDS);

        assertThat(resp.statusCode())
                .as("Expected HTTP %d for GET %s", expectedStatus, path)
                .isEqualTo(expectedStatus);
        return resp.body();
    }
}
