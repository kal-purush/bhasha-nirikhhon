package me.cxis.spring.cache;

import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Component
public class UserCacheManager {

    public String getUserById(String userId) {
        System.out.println("read from db");
        return userId + "xxxxxx";
    }

    public boolean evictUserById(String userId) {
        System.out.println("evictUserById: " + userId);
        return true;
    }

    @Cacheable(value = "simpleMapCacheManager", key = "#userName")
    public String getUserByName(String userName) {
        System.out.println("read user by name from db");
        return userName + "from db";
    }
}