package com.vinaysen.www.io;

import redis.clients.jedis.Jedis;

public class CountStore {
    private static final String COUNT_FIELD = "count";
    private final Jedis jedis;

    public CountStore() {
        this.jedis = new Jedis("localhost");
        this.jedis.set(COUNT_FIELD, "0");
    }


    public long incrementAndGet() {
        return this.jedis.incrBy(COUNT_FIELD, 1);
    }
}