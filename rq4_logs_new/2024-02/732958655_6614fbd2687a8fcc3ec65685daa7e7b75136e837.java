package com.challenger.fridge.redis;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class RedisContainerTest {
    static final String REDIS_IMAGE = "redis:7.2.3-alpine";
    static final GenericContainer<?> REDIS_CONTAINER;

    static {
        REDIS_CONTAINER = new GenericContainer<>(DockerImageName.parse(REDIS_IMAGE)) // (1)
                .withExposedPorts(6379) // (2)
                .withReuse(true); // (3)
        REDIS_CONTAINER.start(); // (4)
        System.setProperty("spring.data.redis.host", REDIS_CONTAINER.getHost());
        System.setProperty("spring.data.redis.port", REDIS_CONTAINER.getMappedPort(6379).toString());
    }

    @DynamicPropertySource // (5)
    public static void overrideProps(DynamicPropertyRegistry registry){
        registry.add("spring.data.redis.host", REDIS_CONTAINER::getHost);
        registry.add("spring.data.redis.port", () -> ""+REDIS_CONTAINER.getMappedPort(6379));
//        registry.add("spring.redis.password", () -> "password");
    }
}