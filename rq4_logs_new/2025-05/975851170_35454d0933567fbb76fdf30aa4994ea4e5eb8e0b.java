package com.example.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    /**
     * RedisTemplate Bean : Redis 서버와 상호작용 Config
     * 
     * key와 value의 직렬화 방식을 지정하여 Redis에 데이터를 저장하고 읽을 때 일관성을 유지합니다.
     *
     * @param connectionFactory Redis 서버와의 연결을 관리하는 팩토리
     * @return 설정된 RedisTemplate 인스턴스
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();

        // 생성된 RedisTemplate에 Redis 서버와 연결을 담당할 ConnectionFactory 주입
        // redisTemplate이 실제 Redis 서버와 통신
        redisTemplate.setConnectionFactory(connectionFactory);

        // -- Key와 Value의 직렬화 방식 설정 --
        // Key 직렬화 - Redis키를 String 타입으로 저장 및 읽기
        redisTemplate.setKeySerializer(new StringRedisSerializer());

        // Value 직렬화 - Redis값을 Object 타입으로 저장(자바 객체 JSON 형태로 직렬화)
        redisTemplate.setValueSerializer(new GenericJackson2JsonRedisSerializer());

        // Hash Key와 Hash Value도 직렬화 방식
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        
        redisTemplate.afterPropertiesSet(); // 모든 속성이 설정된 후 초기화를 수행합니다.
        
        return redisTemplate;
    }
}