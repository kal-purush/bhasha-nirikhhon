package org.petmarket.users.service;

import org.petmarket.users.entity.User;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;

@Service
public class UserCacheService {
    @Caching(evict = {
            @CacheEvict(value = "users", key = "#user.id"),
            @CacheEvict(value = "users", key = "#user.email")
    })
    public void evictCaches(User user) {
    }

    @Caching(evict = {
            @CacheEvict(value = "users", allEntries = true)
    })
    public void evictCaches() {
    }
}