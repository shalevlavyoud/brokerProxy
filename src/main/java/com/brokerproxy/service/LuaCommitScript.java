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
 *   <li><b>SEQ allocation</b> — {@code INCRBY bp:seq:{topic} nChanges};
 *       assigns {@code seq = base - nChanges + i} to each change.</li>
 *   <li><b>Upserts</b> — per item:
 *       {@code HSET state:v}, {@code HSET state:j},
 *       {@code ZADD ch:up seq itemId}, {@code HSET prodver}.</li>
 *   <li><b>Deletes</b> — per itemId:
 *       {@code HDEL state:v}, {@code HDEL state:j},
 *       {@code ZADD ch:del seq itemId}, {@code HDEL prodver}.</li>
 *   <li><b>Recency update</b> — {@code SET recency msgTs}.</li>
 * </ol>
 *
 * <h3>Keys touched (8 KEYS — must all be on the same Redis node)</h3>
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
 *
 * <h3>Returns</h3>
 * <pre>
 *   {status, firstSeq, lastSeq, appliedUpserts, appliedDeletes}
 *   status = "OK" | "FENCED" | "DROPPED_RECENCY"
 * </pre>
 *
 * <h3>TODO BE-07</h3>
 * Replace {@code EVAL} with {@code EVALSHA} (load on startup, reload on NOSCRIPT).
 */
public class LuaCommitScript {

    private static final Logger log = LoggerFactory.getLogger(LuaCommitScript.class);

    // ---- Lua script text --------------------------------------------------------

    /**
     * Lua script executed as a single atomic unit via {@code EVAL}.
     *
     * <p>Invariant enforced: no partial state can be observed — if any of the
     * fencing or recency guards fire, nothing is written to Redis.
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

    public LuaCommitScript(RedisAPI redis, String prefix) {
        this.redis  = redis;
        this.prefix = prefix;
    }

    // ---- Public API -------------------------------------------------------------

    /**
     * Executes the commit script for the given {@link WritePlan}.
     *
     * <p>Uses {@code EVAL} directly (BE-07 will upgrade to {@code EVALSHA}).
     * All I/O is non-blocking; the returned {@link Future} resolves on the
     * Vert.x event loop.
     *
     * @param plan          the diff result to commit
     * @param leaderEpoch   the epoch this instance believes it holds
     * @return {@link CommitResult} — never fails with a Redis error;
     *         Redis I/O errors propagate as a failed {@link Future}
     */
    public Future<CommitResult> eval(WritePlan plan, long leaderEpoch) {
        List<String> args = buildEvalArgs(plan, leaderEpoch);

        return redis.eval(args)
                .map(this::parseResponse)
                .onFailure(err -> log.error(
                        "event=commit.eval_error topic={} producerId={} msgTs={} error=\"{}\"",
                        plan.topic(), plan.producerId(), plan.msgTs(), err.getMessage()));
    }

    // ---- Helpers ----------------------------------------------------------------

    /**
     * Builds the full argument list for {@code EVAL script numkeys [key ...] [arg ...]}.
     */
    private List<String> buildEvalArgs(WritePlan plan, long leaderEpoch) {
        String topic      = plan.topic();
        String producerId = plan.producerId();

        // KEYS
        String epochKey   = prefix + ":leader:epoch";
        String recencyKey = prefix + ":recency:"  + topic + ":" + producerId;
        String seqKey     = prefix + ":seq:"       + topic;
        String stateVKey  = prefix + ":state:v:"   + topic;
        String stateJKey  = prefix + ":state:j:"   + topic;
        String chUpKey    = prefix + ":ch:up:"      + topic;
        String chDelKey   = prefix + ":ch:del:"     + topic;
        String prodVerKey = prefix + ":prodver:"    + topic + ":" + producerId;

        List<String> args = new ArrayList<>();
        args.add(SCRIPT);   // script text
        args.add("8");      // numkeys
        args.add(epochKey);
        args.add(recencyKey);
        args.add(seqKey);
        args.add(stateVKey);
        args.add(stateJKey);
        args.add(chUpKey);
        args.add(chDelKey);
        args.add(prodVerKey);

        // ARGV
        args.add(String.valueOf(leaderEpoch));              // ARGV[1]
        args.add(String.valueOf(plan.msgTs()));             // ARGV[2]
        args.add(String.valueOf(plan.upserts().size()));    // ARGV[3]

        for (var item : plan.upserts()) {                  // ARGV[4 .. 4+nUpserts*3-1]
            args.add(item.itemId());
            args.add(String.valueOf(item.version()));
            args.add(item.dataJson());
        }

        args.add(String.valueOf(plan.deletes().size()));    // ARGV[4+nUpserts*3]
        args.addAll(plan.deletes());                        // ARGV[4+nUpserts*3+1 ..]

        return args;
    }

    /**
     * Parses the Lua multi-bulk reply into a {@link CommitResult}.
     *
     * <p>Reply layout: {@code [status, firstSeq, lastSeq, upserts, deletes]}.
     */
    private CommitResult parseResponse(Response response) {
        String status    = response.get(0).toString();
        long   firstSeq  = response.get(1).toLong();
        long   lastSeq   = response.get(2).toLong();
        int    upserts   = response.get(3).toLong().intValue();
        int    deletes   = response.get(4).toLong().intValue();
        return new CommitResult(CommitStatus.valueOf(status), firstSeq, lastSeq, upserts, deletes);
    }
}
