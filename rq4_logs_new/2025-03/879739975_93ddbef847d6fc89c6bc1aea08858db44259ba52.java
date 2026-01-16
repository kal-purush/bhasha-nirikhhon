package com.example.sideproject.domain.techstack.repository;

import com.example.sideproject.domain.techstack.entity.TechStack;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Slf4j
@Component
public class TechStackCacheRepository {
    private final SetOperations<String, String> redisOperations;
    private final ObjectMapper objectMapper;
    private static final String TECH_STACK_KEY = "tech";

    public TechStackCacheRepository(RedisTemplate<String, String> redisOperations, ObjectMapper objectMapper) {
        this.redisOperations = redisOperations.opsForSet();
        this.objectMapper = objectMapper;
    }

    public List<TechStack> findTechStack() {
        List<TechStack> list = Objects.requireNonNull(redisOperations.members(TECH_STACK_KEY)).stream()
                .map(s -> toObject(s, TechStack.class))
                .toList();
        return list;
    }

    public void save(TechStack techStack) {
        redisOperations.add(TECH_STACK_KEY, toString(techStack));
    }

    public void delete(TechStack techStack) {
        redisOperations.remove(TECH_STACK_KEY, toString(techStack));
    }

    private <T> T toObject(String target, Class<T> type) {
        try {
            return objectMapper.readValue(target, type);
        } catch (JsonProcessingException e) {
            log.error("기술스택 캐시 데이터 조회 중 오류가 발생했습니다.", e);
            throw new RuntimeException(e);
        }
    }

    private String toString(Object target) {
        try {
            return objectMapper.writeValueAsString(target);
        } catch (JsonProcessingException e) {
            log.error("기술스택 캐싱 처리 중 오류가 발생했습니다.", e);
            throw new RuntimeException(e);
        }
    }

    public static String getTechStackKey() {
        return TECH_STACK_KEY;
    }
}