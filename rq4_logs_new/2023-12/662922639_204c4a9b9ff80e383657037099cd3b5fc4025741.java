package com.happiday.Happi_Day.domain.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
@Service
public class RedisService {
    private final Long clientViewCountExpireDurationOfSecond = 86400L;
    private final RedisTemplate<String, Boolean> redisTemplate;

    public boolean isFirstIpRequest(String clientAddress, Long eventId) {
        String key = generateKey(clientAddress, eventId);
        log.info("user event request key: {}", key);
        if (redisTemplate.hasKey(key)) {
            return false;
        }
        return true;
    }

    public void clientRequest(String clientAddress, Long eventId) {
        String key = generateKey(clientAddress, eventId);
        log.info("event request key: {}", key);

        redisTemplate.opsForValue().set(key, true);
        redisTemplate.expire(key, clientViewCountExpireDurationOfSecond, TimeUnit.SECONDS);
    }

    // key 형식 : 'client Address + eventId' ->  '\xac\xed\x00\x05t\x00\x0f127.0.0.1 + 500'
    private String generateKey(String clientAddress, Long eventId) {
        return clientAddress + " + " + eventId;
    }
}