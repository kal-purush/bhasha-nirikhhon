package com.bodytok.healthdiary.config;

import com.bodytok.healthdiary.domain.JwtToken;
import com.bodytok.healthdiary.repository.jwt.JwtTokenRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;

@Configuration
@EnableRedisRepositories(basePackageClasses = {JwtTokenRepository.class})
public class RedisConfiguration {

    @Bean
    public RedisTemplate<String, JwtToken> getRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, JwtToken> redisTemplate = new RedisTemplate<>();
        redisTemplate.setConnectionFactory(redisConnectionFactory);

        return redisTemplate;
    }
}