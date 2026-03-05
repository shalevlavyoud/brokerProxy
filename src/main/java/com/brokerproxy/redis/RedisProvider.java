package com.brokerproxy.redis;

import com.brokerproxy.config.AppConfig;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetClientOptions;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;

/**
 * Factory for the Vert.x Redis client.
 *
 * <p>Creates a pooled {@link Redis} client from {@link AppConfig} and wraps it in a
 * {@link RedisAPI} for typed command access. The pool handles reconnection and
 * connection reuse automatically.
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@code redis.connectTimeout} — TCP connect timeout (ms); default 5 000</li>
 *   <li>{@code redis.password}       — AUTH password; omit or {@code null} for none</li>
 *   <li>{@code redis.ssl}            — set {@code true} to enable TLS</li>
 * </ul>
 *
 * <p>For production (Sentinel topology), the connection string should use the
 * {@code redis-sentinel://} scheme and {@link RedisOptions#setMasterName(String)}.
 * A plain {@code redis://} URL is used for local development and tests.
 */
public final class RedisProvider {

    private RedisProvider() {}

    /**
     * Creates a pooled {@link RedisAPI} backed by the configured Redis endpoint.
     *
     * <p>The returned {@link RedisAPI} wraps a pool; each command acquires a connection
     * from the pool, executes, and returns it — no explicit connection management needed
     * by callers.
     */
    public static RedisAPI createApi(Vertx vertx, AppConfig config) {
        NetClientOptions netOpts = new NetClientOptions()
                .setConnectTimeout(config.redisConnectTimeoutMs());

        if (config.redisSsl()) {
            netOpts.setSsl(true);
        }

        // Build connection string, embedding password when provided
        String connStr;
        String password = config.redisPassword();
        if (password != null && !password.isEmpty()) {
            // redis://:password@host:port
            connStr = "redis://:" + password + "@"
                    + config.redisHost() + ":" + config.redisPort();
        } else {
            connStr = "redis://" + config.redisHost() + ":" + config.redisPort();
        }

        RedisOptions opts = new RedisOptions()
                .setConnectionString(connStr)
                .setNetClientOptions(netOpts)
                .setMaxPoolSize(4)
                .setMaxPoolWaiting(32);

        return RedisAPI.api(Redis.createClient(vertx, opts));
    }
}
