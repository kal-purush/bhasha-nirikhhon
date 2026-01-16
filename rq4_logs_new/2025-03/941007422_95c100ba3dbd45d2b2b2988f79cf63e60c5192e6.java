package com.example.auction.Global.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

   @Value("${spring.data.redis.host}")
    private String redisHost;

    @Value("${spring.data.redis.port}")
    private int redisPort;

    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        return new LettuceConnectionFactory(redisHost, redisPort); // Lettuce 클라이언트를 사용해 Redis 연결 팩토리 초기화
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory); // Redis 연결 팩토리 설정
        template.setKeySerializer(new StringRedisSerializer()); // 키를 직렬화할 때 문자열로 직렬화하는 설정
        template.setValueSerializer(new GenericToStringSerializer<>(Object.class));  // 값을 직렬화할 때 기본 객체 직렬화를 사용하는 설정
        return template;
    }


    @Bean
    public ZSetOperations<String, Object> zSetOperations(RedisTemplate<String, Object> redisTemplate) {
        // ZSetOperations는 Redis의 Sorted Set 데이터 구조를 다루는 인터페이스
        return redisTemplate.opsForZSet();  // RedisTemplate에서 ZSetOperations 빈 생성 및 반환
    }
}
