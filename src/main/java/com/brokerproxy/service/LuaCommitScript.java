package com.brokerproxy.service;

import com.brokerproxy.model.CommitResult;
import com.brokerproxy.model.CommitResult.CommitStatus;
import com.brokerproxy.model.WritePlan;
import io.vertx.core.Future;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Executes the atomic Lua commit script against Redis.
 *
 * <h3>What the script does (single EVAL = single atomic unit)</h3>
 * <ol>
 *   <li><b>Fencing check</b> — compares {@code expectedEpoch} with
 *       {@code bp:leader:epoch}; returns {@code FENCED} if they differ.</li>
 *   <li><b>Recency check</b> — verifies {@code msgTs > bp:recency:{topic}:{producerId}};
 *       returns {@code DROPPED_RECENCY} if not.</li>
 *   <li><b>SEQ allocation</b> — {@code INCRBY bp:seq:{topic} nChanges}.</li>
 *   <li><b>Upserts/Deletes</b> — state, change-indices, prodver cache.</li>
 *   <li><b>Recency update</b> — {@code SET recency msgTs}.</li>
 * </ol>
 *
 * <h3>EVALSHA lifecycle (BE-07)</h3>
 * <ol>
 *   <li>Call {@link #loadScript()} once at verticle startup — runs
 *       {@code SCRIPT LOAD} and caches the SHA1.</li>
 *   <li>Every {@link #eval} call uses {@code EVALSHA sha …} (fast path).</li>
 *   <li>If Redis returns {@code NOSCRIPT} (script evicted), the SHA is reloaded
 *       automatically and the {@code EVALSHA} is retried once.</li>
 *   <li>If {@link #loadScript()} has not yet been called, {@code EVAL} is used
 *       as a fallback (start-up race guard).</li>
 * </ol>
 *
 * <h3>Keys touched (8 KEYS)</h3>
 * <pre>
 *   KEYS[1]  bp:leader:epoch
 *   KEYS[2]  bp:recency:{topic}:{producerId}
 *   KEYS[3]  bp:seq:{topic}
 *   KEYS[4]  bp:state:v:{topic}
 *   KEYS[5]  bp:state:j:{topic}
 *   KEYS[6]  bp:ch:up:{topic}
 *   KEYS[7]  bp:ch:del:{topic}
 *   KEYS[8]  bp:prodver:{topic}:{producerId}
 * </pre>
 *
 * <h3>ARGV layout</h3>
 * <pre>
 *   ARGV[1]                          expectedEpoch
 *   ARGV[2]                          msgTs
 *   ARGV[3]                          nUpserts
 *   ARGV[4 .. 4+nUpserts*3-1]        upserts: (itemId, version, dataJson) × nUpserts
 *   ARGV[4+nUpserts*3]               nDeletes
 *   ARGV[4+nUpserts*3+1 ..]          deletes: itemId × nDeletes
 * </pre>
 */
public class LuaCommitScript {

    private static final Logger log = LoggerFactory.getLogger(LuaCommitScript.class);

    // ---- Lua script text --------------------------------------------------------

    /**
     * The Lua commit script.  Kept as a constant so callers can run
     * {@code SCRIPT LOAD} and receive a SHA to use with {@code EVALSHA}.
     *
     * <p>Invariant: no partial state can be observed — all guards (fencing,
     * recency) abort with zero writes if triggered.
     */
    static final String SCRIPT = """
            -- bp-commit-v1: fencing + recency + seq + state + indices (atomic)
            local storedEpoch = redis.call('GET', KEYS[1])
            if storedEpoch == false then storedEpoch = '0' end
            if storedEpoch ~= ARGV[1] then
                return {'FENCED', -1, -1, 0, 0}
            end
            local lastAccepted = redis.call('GET', KEYS[2])
            if lastAccepted ~= false then
                if tonumber(ARGV[2]) <= tonumber(lastAccepted) then
                    return {'DROPPED_RECENCY', -1, -1, 0, 0}
                end
            end
            local nUpserts      = tonumber(ARGV[3])
            local deleteArgBase = 4 + nUpserts * 3
            local nDeletes      = tonumber(ARGV[deleteArgBase])
            local nChanges      = nUpserts + nDeletes
            local firstSeq = -1
            local lastSeq  = -1
            local seqBase  = 0
            if nChanges > 0 then
                local topSeq = tonumber(redis.call('INCRBY', KEYS[3], nChanges))
                seqBase  = topSeq - nChanges
                firstSeq = seqBase + 1
                lastSeq  = topSeq
            end
            for i = 0, nUpserts - 1 do
                local base     = 4 + i * 3
                local itemId   = ARGV[base]
                local version  = ARGV[base + 1]
                local dataJson = ARGV[base + 2]
                local seq      = seqBase + i + 1
                redis.call('HSET', KEYS[4], itemId, version)
                redis.call('HSET', KEYS[5], itemId, dataJson)
                redis.call('ZADD', KEYS[6], seq, itemId)
                redis.call('HSET', KEYS[8], itemId, version)
            end
            for i = 0, nDeletes - 1 do
                local itemId = ARGV[deleteArgBase + 1 + i]
                local seq    = seqBase + nUpserts + i + 1
                redis.call('HDEL', KEYS[4], itemId)
                redis.call('HDEL', KEYS[5], itemId)
                redis.call('ZADD', KEYS[7], seq, itemId)
                redis.call('HDEL', KEYS[8], itemId)
            end
            redis.call('SET', KEYS[2], ARGV[2])
            return {'OK', firstSeq, lastSeq, nUpserts, nDeletes}
            """;

    // ---- Instance state ---------------------------------------------------------

    private final RedisAPI redis;
    private final String   prefix;

    /**
     * Cached SHA1 from {@code SCRIPT LOAD}.  {@code null} until {@link #loadScript}
     * completes.  Written once (or on NOSCRIPT reload); read on every eval call.
     * Declared {@code volatile} so the write is visible across event-loop threads
     * on multi-core deployments.
     */
    private volatile String sha = null;

    public LuaCommitScript(RedisAPI redis, String prefix) {
        this.redis  = redis;
        this.prefix = prefix;
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Pre-loads the script into the Redis script cache via {@code SCRIPT LOAD}.
     *
     * <p>Must be called once at verticle startup.  Subsequent commits will use
     * the cached SHA via {@code EVALSHA} (faster than {@code EVAL} — skips
     * script parsing on every call).
     *
     * @return a {@link Future} that completes when the SHA has been cached
     */
    public Future<Void> loadScript() {
        return redis.script(List.of("LOAD", SCRIPT))
                .map(response -> {
                    sha = response.toString();
                    log.info("event=commit_script.loaded sha={}", sha);
                    return (Void) null;
                })
                .onFailure(err -> log.error(
                        "event=commit_script.load_failed error=\"{}\"", err.getMessage()));
    }

    /**
     * Executes the commit script for the given {@link WritePlan}.
     *
     * <p>Uses {@code EVALSHA} when the SHA is cached; falls back to {@code EVAL}
     * otherwise.  On a {@code NOSCRIPT} error (script evicted from Redis cache),
     * reloads the SHA and retries {@code EVALSHA} once automatically.
     *
     * @param plan          the diff result to commit
     * @param leaderEpoch   the epoch this instance believes it holds
     */
    public Future<CommitResult> eval(WritePlan plan, long leaderEpoch) {
        List<String> common = buildCommonArgs(plan, leaderEpoch);
        String currentSha   = this.sha;

        if (currentSha != null) {
            return evalSha(currentSha, common)
                    .recover(err -> handleNoscript(err, common))
                    .map(this::parseResponse);
        }

        // SHA not yet loaded — use EVAL as fallback
        log.debug("event=commit_script.eval_fallback topic={} producerId={}",
                plan.topic(), plan.producerId());
        List<String> evalArgs = new ArrayList<>();
        evalArgs.add(SCRIPT);
        evalArgs.addAll(common);
        return redis.eval(evalArgs)
                .map(this::parseResponse)
                .onFailure(err -> log.error(
                        "event=commit.eval_error topic={} producerId={} error=\"{}\"",
                        plan.topic(), plan.producerId(), err.getMessage()));
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Executes {@code EVALSHA sha numkeys keys... argv...}.
     */
    private Future<Response> evalSha(String scriptSha, List<String> common) {
        List<String> args = new ArrayList<>();
        args.add(scriptSha);
        args.addAll(common);
        return redis.evalsha(args);
    }

    /**
     * Handles a {@code NOSCRIPT} error by reloading the script and retrying
     * {@code EVALSHA} once.  All other errors are propagated as-is.
     */
    private Future<Response> handleNoscript(Throwable err, List<String> common) {
        if (err.getMessage() != null && err.getMessage().contains("NOSCRIPT")) {
            log.warn("event=commit_script.noscript_reload");
            return loadScript()
                    .compose(v -> evalSha(this.sha, common));
        }
        return Future.failedFuture(err);
    }

    /**
     * Builds the common argument segment shared by {@code EVAL} and {@code EVALSHA}:
     * {@code [numkeys, key1..key8, argv1..argN]}.
     *
     * <p>The script text or SHA is prepended by the caller.
     */
    private List<String> buildCommonArgs(WritePlan plan, long leaderEpoch) {
        String topic      = plan.topic();
        String producerId = plan.producerId();

        List<String> args = new ArrayList<>();
        args.add("8");   // numkeys
        args.add(prefix + ":leader:epoch");
        args.add(prefix + ":recency:"  + topic + ":" + producerId);
        args.add(prefix + ":seq:"       + topic);
        args.add(prefix + ":state:v:"   + topic);
        args.add(prefix + ":state:j:"   + topic);
        args.add(prefix + ":ch:up:"     + topic);
        args.add(prefix + ":ch:del:"    + topic);
        args.add(prefix + ":prodver:"   + topic + ":" + producerId);

        args.add(String.valueOf(leaderEpoch));
        args.add(String.valueOf(plan.msgTs()));
        args.add(String.valueOf(plan.upserts().size()));

        for (var item : plan.upserts()) {
            args.add(item.itemId());
            args.add(String.valueOf(item.version()));
            args.add(item.dataJson());
        }

        args.add(String.valueOf(plan.deletes().size()));
        args.addAll(plan.deletes());

        return args;
    }

    /**
     * Parses the Lua multi-bulk reply into a {@link CommitResult}.
     * Reply layout: {@code [status, firstSeq, lastSeq, upserts, deletes]}.
     */
    private CommitResult parseResponse(Response response) {
        String status   = response.get(0).toString();
        long   firstSeq = response.get(1).toLong();
        long   lastSeq  = response.get(2).toLong();
        int    upserts  = response.get(3).toLong().intValue();
        int    deletes  = response.get(4).toLong().intValue();
        return new CommitResult(CommitStatus.valueOf(status), firstSeq, lastSeq, upserts, deletes);
    }
}
