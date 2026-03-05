package com.brokerproxy.service;

import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only service backing {@code GET /changes}.
 *
 * <h3>Redis key model (all reads — no writes)</h3>
 * <pre>
 * Key                     Type    Purpose
 * bp:seq:min:{topic}      String  Minimum retained seq; sinceSeq below this → fullResyncRequired
 * bp:ch:up:{topic}        ZSet    Upserted itemIds scored by seq
 * bp:ch:del:{topic}       ZSet    Deleted itemIds scored by seq
 * bp:state:v:{topic}      Hash    itemId → current version (long)
 * bp:state:j:{topic}      Hash    itemId → current dataJson (String)
 * </pre>
 *
 * <h3>Algorithm</h3>
 * <ol>
 *   <li>GET {@code bp:seq:min:{topic}} — if {@code sinceSeq < minRetained} return fullResync immediately.</li>
 *   <li>ZRANGEBYSCORE both up and del ZSets with exclusive lower bound {@code (sinceSeq} and LIMIT.</li>
 *   <li>Conflict resolution: for items in both sets, the entry with the <em>higher</em> seq wins.</li>
 *   <li>HMGET {@code bp:state:v} and {@code bp:state:j} for all final upsert ids (parallel).</li>
 *   <li>Skip upsert items whose state entry is absent (deleted after the index write).</li>
 *   <li>{@code newSeq} = max(all returned seq values); falls back to {@code sinceSeq} when empty (BE-09).</li>
 * </ol>
 *
 * <p>All Redis calls are non-blocking; no event-loop blocking occurs.
 */
public class ChangesService {

    private static final Logger log = LoggerFactory.getLogger(ChangesService.class);

    private final RedisAPI redis;
    private final String   prefix;

    public ChangesService(RedisAPI redis, String prefix) {
        this.redis  = redis;
        this.prefix = prefix;
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Queries changes for {@code topic} with {@code seq > sinceSeq}, returning at most
     * {@code limit} entries from each change index.
     *
     * @param topic    topic name (already validated by the handler)
     * @param sinceSeq exclusive lower bound on the seq; must be ≥ 0
     * @param limit    max entries to fetch from each ZSet
     * @return a {@link Future} resolving to a {@link ChangesResponse}
     */
    public Future<ChangesResponse> query(String topic, long sinceSeq, int limit) {
        String minKey   = prefix + ":seq:min:" + topic;
        String upKey    = prefix + ":ch:up:"   + topic;
        String delKey   = prefix + ":ch:del:"  + topic;
        String scoreMin = "(" + sinceSeq;
        String limitStr = String.valueOf(limit);

        // Step 1 — check minimum retained seq
        return redis.get(minKey)
                .compose(minResp -> {
                    long minRetained = parseMinRetained(minResp);
                    if (sinceSeq < minRetained) {
                        log.debug("event=changes.full_resync_required topic={} sinceSeq={} minRetained={}",
                                topic, sinceSeq, minRetained);
                        return Future.succeededFuture(ChangesResponse.fullResync(sinceSeq));
                    }

                    // Step 2 — parallel ZRANGEBYSCORE for ups and dels
                    Future<Response> upsFut = redis.zrangebyscore(List.of(
                            upKey, scoreMin, "+inf", "WITHSCORES", "LIMIT", "0", limitStr));
                    Future<Response> delsFut = redis.zrangebyscore(List.of(
                            delKey, scoreMin, "+inf", "WITHSCORES", "LIMIT", "0", limitStr));

                    return Future.all(upsFut, delsFut)
                            .compose(cf -> buildResponse(topic, sinceSeq, cf.resultAt(0), cf.resultAt(1)));
                })
                .onFailure(err -> log.error(
                        "event=changes.query_error topic={} sinceSeq={} error=\"{}\"",
                        topic, sinceSeq, err.getMessage()));
    }

    // ---- Private helpers --------------------------------------------------------

    /**
     * Resolves conflicts, fetches state, and assembles the final {@link ChangesResponse}.
     *
     * @param upsResp  raw ZRANGEBYSCORE WITHSCORES response for the up-index
     * @param delsResp raw ZRANGEBYSCORE WITHSCORES response for the del-index
     */
    private Future<ChangesResponse> buildResponse(String topic, long sinceSeq,
                                                   Response upsResp, Response delsResp) {
        // Step 3 — parse [member, score, member, score, ...] arrays
        Map<String, Long> upMap  = parseZrangeWithScores(upsResp);
        Map<String, Long> delMap = parseZrangeWithScores(delsResp);

        // Step 4 — conflict resolution: keep the entry with the higher seq
        Map<String, Long> finalUps  = new LinkedHashMap<>();
        Map<String, Long> finalDels = new LinkedHashMap<>();

        for (Map.Entry<String, Long> e : upMap.entrySet()) {
            String id  = e.getKey();
            long   seq = e.getValue();
            if (seq > delMap.getOrDefault(id, -1L)) {
                finalUps.put(id, seq);
            }
        }
        for (Map.Entry<String, Long> e : delMap.entrySet()) {
            String id  = e.getKey();
            long   seq = e.getValue();
            if (seq > upMap.getOrDefault(id, -1L)) {
                finalDels.put(id, seq);
            }
        }

        // Step 5 — HMGET state hashes for upsert ids (skip if no ups to avoid empty HMGET)
        if (finalUps.isEmpty()) {
            long newSeq = computeNewSeq(sinceSeq, finalUps, finalDels);
            return Future.succeededFuture(
                    new ChangesResponse(List.of(), new ArrayList<>(finalDels.keySet()), newSeq, false));
        }

        List<String> upsIds    = new ArrayList<>(finalUps.keySet());
        String       stateVKey = prefix + ":state:v:" + topic;
        String       stateJKey = prefix + ":state:j:" + topic;

        List<String> hmgetVArgs = new ArrayList<>(upsIds.size() + 1);
        hmgetVArgs.add(stateVKey);
        hmgetVArgs.addAll(upsIds);

        List<String> hmgetJArgs = new ArrayList<>(upsIds.size() + 1);
        hmgetJArgs.add(stateJKey);
        hmgetJArgs.addAll(upsIds);

        Future<Response> vFut = redis.hmget(hmgetVArgs);
        Future<Response> jFut = redis.hmget(hmgetJArgs);

        return Future.all(vFut, jFut)
                .map(hmgetCf -> assembleItems(topic, sinceSeq, upsIds, finalUps, finalDels,
                        hmgetCf.resultAt(0), hmgetCf.resultAt(1)));
    }

    /**
     * Assembles {@link ChangesItem} list from HMGET responses, skipping any id whose
     * state entry is absent (item was indexed for upsert but since deleted from state).
     */
    private ChangesResponse assembleItems(String topic, long sinceSeq,
                                           List<String> upsIds,
                                           Map<String, Long> finalUps,
                                           Map<String, Long> finalDels,
                                           Response vResp, Response jResp) {
        List<ChangesItem> items = new ArrayList<>(upsIds.size());

        for (int i = 0; i < upsIds.size(); i++) {
            // HMGET returns null for fields not present in the hash (Vert.x 4.5.x: Java null check)
            Response vEntry = (vResp != null && i < vResp.size()) ? vResp.get(i) : null;
            Response jEntry = (jResp != null && i < jResp.size()) ? jResp.get(i) : null;

            if (vEntry == null || jEntry == null) {
                // State was removed after the change-index write — skip silently
                log.debug("event=changes.state_miss topic={} itemId={} — skipped from items",
                        topic, upsIds.get(i));
                continue;
            }

            String itemId   = upsIds.get(i);
            long   version  = Long.parseLong(vEntry.toString());
            String dataJson = jEntry.toString();
            items.add(new ChangesItem(itemId, version, dataJson));
        }

        long newSeq = computeNewSeq(sinceSeq, finalUps, finalDels);
        return new ChangesResponse(items, new ArrayList<>(finalDels.keySet()), newSeq, false);
    }

    // ---- Static parsing helpers -------------------------------------------------

    /**
     * Parses a ZRANGEBYSCORE WITHSCORES response into an ordered {@code itemId → seq} map.
     *
     * <h4>Format variants</h4>
     * <ul>
     *   <li><b>RESP2 (flat)</b>: {@code [member₀, score₀, member₁, score₁, …]}
     *       — each element is a {@link ResponseType#BULK} string.</li>
     *   <li><b>RESP3 (pairs)</b>: {@code [[member₀, score₀], [member₁, score₁], …]}
     *       — each element is a {@link ResponseType#MULTI} 2-element array.
     *       Redis 7.x + Vert.x 4.5.x negotiates RESP3 by default.</li>
     * </ul>
     *
     * <p>Scores arrive as floating-point strings (e.g. {@code "5"} or {@code "5.0"})
     * and are parsed via {@link Double} then cast to {@code long}.
     */
    static Map<String, Long> parseZrangeWithScores(Response resp) {
        Map<String, Long> result = new LinkedHashMap<>();
        if (resp == null || resp.size() == 0) return result;

        Response firstElem = resp.get(0);
        if (firstElem != null && firstElem.type() == ResponseType.MULTI) {
            // RESP3: each element is a [member, score] pair
            for (int i = 0; i < resp.size(); i++) {
                Response pair   = resp.get(i);
                String   member = pair.get(0).toString();
                long     seq    = (long) Double.parseDouble(pair.get(1).toString());
                result.put(member, seq);
            }
        } else {
            // RESP2: flat [member, score, member, score, ...] array
            for (int i = 0; i + 1 < resp.size(); i += 2) {
                String member = resp.get(i).toString();
                long   seq    = (long) Double.parseDouble(resp.get(i + 1).toString());
                result.put(member, seq);
            }
        }
        return result;
    }

    /** Returns {@code max(sinceSeq, max-seq-in-ups, max-seq-in-dels)} (BE-09). */
    static long computeNewSeq(long sinceSeq, Map<String, Long> ups, Map<String, Long> dels) {
        long max = sinceSeq;
        for (long v : ups.values())  max = Math.max(max, v);
        for (long v : dels.values()) max = Math.max(max, v);
        return max;
    }

    /** Parses the {@code bp:seq:min:{topic}} string; returns 0 when the key is absent. */
    static long parseMinRetained(Response resp) {
        if (resp == null) return 0L;
        return Long.parseLong(resp.toString());
    }

    // ---- Response value types ---------------------------------------------------

    /**
     * A single upserted item returned in the {@code items[]} array.
     *
     * @param itemId   item identifier
     * @param version  current version number
     * @param dataJson raw JSON payload
     */
    public record ChangesItem(String itemId, long version, String dataJson) {}

    /**
     * Full response payload for {@code GET /changes}.
     *
     * @param items              upserted/updated items
     * @param deleted            itemIds that were deleted
     * @param newSeq             max seq across all returned entries; equals {@code sinceSeq} when empty
     * @param fullResyncRequired {@code true} when {@code sinceSeq} predates the retention window
     */
    public record ChangesResponse(
            List<ChangesItem> items,
            List<String>      deleted,
            long              newSeq,
            boolean           fullResyncRequired) {

        /** Convenience factory for the full-resync sentinel response. */
        public static ChangesResponse fullResync(long sinceSeq) {
            return new ChangesResponse(List.of(), List.of(), sinceSeq, true);
        }
    }
}
