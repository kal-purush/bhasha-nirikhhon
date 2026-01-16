package net.trellisframework.data.caffeine.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import net.trellisframework.boot.cache.core.constant.CacheManagers;
import net.trellisframework.boot.cache.core.payload.TTL;
import net.trellisframework.boot.cache.core.scanner.AnnotationScanner;
import net.trellisframework.core.application.ApplicationContextProvider;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Configuration
@ImportAutoConfiguration(ApplicationContextProvider.class)
public class CaffeineCacheConfiguration {

    @Bean(CacheManagers.CAFFEINE)
    public CacheManager cacheManager() {
        WildcardCaffeineCacheManager caffeine = new WildcardCaffeineCacheManager();
        caffeine.setCaffeine(Caffeine.newBuilder().expireAfterWrite(1, TimeUnit.DAYS).initialCapacity(10000));
        Set<TTL> elements = AnnotationScanner.ttl();
        for (TTL element : elements) {
            Arrays.stream(element.getName()).forEach(name -> caffeine.registerCustomCache(name, Caffeine.newBuilder().expireAfterWrite(element.getTtl(), element.getUnit()).initialCapacity(1000).build()));
        }
        return caffeine;
    }
}