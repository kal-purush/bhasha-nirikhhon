package org.example.config.cache;

import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachingConfigurerSupport;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheManager;

import javax.annotation.Resource;

/**
 * @author linzhaoming
 * @since 2020-12-18
 **/
@Configuration
@EnableCaching
public class CacheConfig extends CachingConfigurerSupport {

    @Resource
    private RedisCacheManager redisCacheManager;

    @Bean
    @Override
    public CacheManager cacheManager() {
        return redisCacheManager;
    }

}