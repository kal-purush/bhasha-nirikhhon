package com.roomfit.be.redis.application;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class ReactiveRedisService {
    private final ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;

    public Mono<Boolean> save(String key, Object value) {
        return reactiveRedisTemplate.opsForValue().set(key, value);
    }
    public Mono<Boolean> saveWithTTL(String key, Object value, long timeoutInSeconds) {
        return reactiveRedisTemplate.opsForValue()
                .set(key, value, Duration.ofSeconds(timeoutInSeconds));
    }
    public Mono<Object> get(String key) {
        return reactiveRedisTemplate.opsForValue().get(key);
    }

    public Mono<Boolean> delete(String key) {
        return reactiveRedisTemplate.delete(key).map(count -> count > 0);
    }
}