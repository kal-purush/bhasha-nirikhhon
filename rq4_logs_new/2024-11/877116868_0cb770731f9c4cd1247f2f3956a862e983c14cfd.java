package com.eng.service;

import com.eng.domain.Study;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class RedisService {
    private final RedisTemplate<String, Integer> numTemplate;
    private final RedisTemplate<String, List<Study>> redisTemplate;

    public void addStudyList(String username, List<Study> list){
        redisTemplate.opsForValue().set(username+"list", list);
    }

    public List<Study> getStudyList(String username){
        return redisTemplate.opsForValue().get(username+"list");
    }

    public void addMaxPage(String username, int maxPage){
        numTemplate.opsForValue().set(username+"page", maxPage);
    }
    public Integer getMaxPage(String username){
        return numTemplate.opsForValue().get(username+"page");
    }
}