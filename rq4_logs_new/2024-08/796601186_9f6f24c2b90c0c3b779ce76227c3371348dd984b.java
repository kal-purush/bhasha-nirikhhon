package com.everycare.backend.domain.chatbot.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class ChatSessionService {

    private final RedisTemplate<String, Object> redisTemplate;

    @Autowired
    public ChatSessionService(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void saveChatHistory(String sessionId, String chatHistory) {
        ValueOperations<String, Object> ops = redisTemplate.opsForValue();
        ops.set(sessionId, chatHistory, Duration.ofHours(1)); // 세션 기록을 1시간 동안 유지
    }

    public String getChatHistory(String sessionId) {
        ValueOperations<String, Object> ops = redisTemplate.opsForValue();
        return (String) ops.get(sessionId);
    }

    public void deleteChatHistory(String sessionId) {
        redisTemplate.delete(sessionId);
    }
}