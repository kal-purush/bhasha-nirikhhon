package com.happiday.Happi_Day.config;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import redis.embedded.RedisServer;

import java.io.IOException;

@DisplayName("Embedded Redis 설정")
@Profile("test")
@Configuration
public class EmbeddedRedisConfig {

    @Value("${spring.data.redis.port}")
    private static int port;

    private static RedisServer redisServer;

    @BeforeAll
    public static void startRedis() throws IOException {
        redisServer = new RedisServer(port);
        redisServer.start();
    }
}