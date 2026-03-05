package com.brokerproxy.service;

import io.vertx.redis.client.Response;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AbstractDiffEngine#parseHgetall} — pure utility, no Redis.
 *
 * <h3>Edge cases covered</h3>
 * <ol>
 *   <li>Null response → empty map (Redis key absent)</li>
 *   <li>Empty response (size 0) → empty map (empty Hash)</li>
 *   <li>Two field-value pairs → correctly populated map</li>
 *   <li>Corrupt odd-element response (size 3) → only the complete pair is read;
 *       the trailing orphan field is silently ignored by the
 *       {@code i + 1 < response.size()} guard</li>
 * </ol>
 *
 * <p>{@code parseHgetall} is {@code protected static} and accessible from the
 * same package, so no subclass is needed to exercise it.
 */
class AbstractDiffEngineTest {

    // ---- null response ----------------------------------------------------------

    @Test
    @DisplayName("parseHgetall: null response → empty map")
    void null_response_returns_empty_map() {
        assertThat(AbstractDiffEngine.parseHgetall(null)).isEmpty();
    }

    // ---- empty response ---------------------------------------------------------

    @Test
    @DisplayName("parseHgetall: response with size=0 → empty map")
    void empty_response_returns_empty_map() {
        Response resp = mock(Response.class);
        when(resp.size()).thenReturn(0);

        assertThat(AbstractDiffEngine.parseHgetall(resp)).isEmpty();
    }

    // ---- two field-value pairs --------------------------------------------------

    @Test
    @DisplayName("parseHgetall: two field-value pairs → correctly populated map")
    void two_pairs_are_parsed_correctly() {
        Response resp = mockHgetallResponse(
                new String[]{"field-a", "10", "field-b", "20"});

        Map<String, Long> result = AbstractDiffEngine.parseHgetall(resp);

        assertThat(result).hasSize(2);
        assertThat(result).containsEntry("field-a", 10L);
        assertThat(result).containsEntry("field-b", 20L);
    }

    // ---- odd-element / corrupt response -----------------------------------------

    @Test
    @DisplayName("parseHgetall: size=3 (odd) → only the first complete pair is returned")
    void odd_element_response_reads_only_complete_pairs() {
        // Simulates a corrupt Redis response: [field-a, 10, field-orphan]
        // The guard `i + 1 < response.size()` means we only process i=0 (pairs at 0,1)
        // and skip i=2 (no element at index 3).
        Response field0   = mockStringResponse("field-a");
        Response val0     = mockStringResponse("10");
        Response orphan   = mockStringResponse("field-orphan");  // no paired value

        Response resp = mock(Response.class);
        when(resp.size()).thenReturn(3);
        when(resp.get(0)).thenReturn(field0);
        when(resp.get(1)).thenReturn(val0);
        when(resp.get(2)).thenReturn(orphan);

        Map<String, Long> result = AbstractDiffEngine.parseHgetall(resp);

        assertThat(result).hasSize(1);
        assertThat(result).containsEntry("field-a", 10L);
        assertThat(result).doesNotContainKey("field-orphan");
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Builds a mock {@link Response} representing an HGETALL flat array.
     * Elements are provided as alternating field/value strings.
     *
     * <p>All element mocks are created <em>before</em> any stubbing begins so
     * Mockito does not see nested {@code mock()} calls inside a
     * {@code when(...).thenReturn(...)} chain (which causes
     * {@code UnfinishedStubbingException}).
     */
    private static Response mockHgetallResponse(String[] elements) {
        // Create all child mocks first
        Response[] elementMocks = new Response[elements.length];
        for (int i = 0; i < elements.length; i++) {
            elementMocks[i] = mockStringResponse(elements[i]);
        }
        // Now wire stubs on the parent response
        Response resp = mock(Response.class);
        when(resp.size()).thenReturn(elements.length);
        for (int i = 0; i < elements.length; i++) {
            when(resp.get(i)).thenReturn(elementMocks[i]);
        }
        return resp;
    }

    /** Returns a {@link Response} mock whose {@code toString()} returns {@code value}. */
    private static Response mockStringResponse(String value) {
        Response r = mock(Response.class);
        when(r.toString()).thenReturn(value);
        return r;
    }
}
