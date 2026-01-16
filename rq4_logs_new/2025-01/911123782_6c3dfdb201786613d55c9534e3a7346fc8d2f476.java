package com.roomfit.be.global.config;

import com.roomfit.be.auth.domain.VerificationCode;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {
    @Value("${spring.data.redis.host}")
    private String redisHost;

    @Value("${spring.data.redis.port}")
    private int redisPort;
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {
        return new LettuceConnectionFactory(redisHost, redisPort);
    }

    @Bean
    @Qualifier("reactiveRedisTemplate")
    public <T> ReactiveRedisTemplate<String, T> reactiveRedisTemplate(
            RedisConnectionFactory connectionFactory, Class<T> type) {

        Jackson2JsonRedisSerializer<T> serializer = new Jackson2JsonRedisSerializer<>(type);

        return new ReactiveRedisTemplate<>(
                (ReactiveRedisConnectionFactory) connectionFactory,
                RedisSerializationContext.<String, T>newSerializationContext()
                        .key(new StringRedisSerializer())
                        .value(RedisSerializationContext.SerializationPair.fromSerializer(serializer))
                        .hashKey(new StringRedisSerializer())
                        .hashValue(serializer)
                        .build()
        );
    }
    @Bean
    @Qualifier("verificationCodeRedisTemplate")
    public ReactiveRedisTemplate<String, VerificationCode> verificationCodeRedisTemplate(
            RedisConnectionFactory connectionFactory) {
        return reactiveRedisTemplate(connectionFactory, VerificationCode.class);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(
            RedisConnectionFactory connectionFactory) {

        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new GenericJackson2JsonRedisSerializer());
        return template;
    }

    @Bean
    public ReactiveHashOperations<String, String, Object> reactiveHashOperations(
            ReactiveRedisTemplate<String, Object> reactiveRedisTemplate) {
        return reactiveRedisTemplate.opsForHash();
    }
}