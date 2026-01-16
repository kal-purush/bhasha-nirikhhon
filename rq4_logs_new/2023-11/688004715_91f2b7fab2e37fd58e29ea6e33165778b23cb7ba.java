package kr.lnsc.api.config;

import com.google.code.ssm.Cache;
import com.google.code.ssm.CacheFactory;
import com.google.code.ssm.config.AddressProvider;
import com.google.code.ssm.config.DefaultAddressProvider;
import com.google.code.ssm.providers.CacheConfiguration;
import com.google.code.ssm.providers.xmemcached.MemcacheClientFactoryImpl;
import com.google.code.ssm.spring.ExtendedSSMCacheManager;
import com.google.code.ssm.spring.SSMCache;
import com.google.code.ssm.spring.SSMCacheManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableCaching
@EnableAspectJAutoProxy
public class CacheConfig {

    @Bean
    @ConditionalOnProperty(name = "spring.cache.type", havingValue = "memcached")
    public CacheManager cacheManager(@Value("${cache.address:localhost:11211}") String address,
                                     @Value("${cache.operationTimeout:500}") int operationTimeout,
                                     @Value("${cache.cacheName:link}") String cacheName,
                                     @Value("${cache.expiration:1800}") int expiration) {
        CacheConfiguration cacheConfiguration = new CacheConfiguration();
        cacheConfiguration.setConsistentHashing(true);
        cacheConfiguration.setUseBinaryProtocol(true);
        cacheConfiguration.setOperationTimeout(operationTimeout);
        cacheConfiguration.setUseNameAsKeyPrefix(true);
        cacheConfiguration.setKeyPrefixSeparator(":");

        MemcacheClientFactoryImpl cacheClientFactory = new MemcacheClientFactoryImpl();
        AddressProvider addressProvider = new DefaultAddressProvider(address);

        CacheFactory cacheFactory = new CacheFactory();
        cacheFactory.setCacheName(cacheName);
        cacheFactory.setCacheClientFactory(cacheClientFactory);
        cacheFactory.setAddressProvider(addressProvider);
        cacheFactory.setConfiguration(cacheConfiguration);

        Cache cache;
        try {
            cache = cacheFactory.getObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SSMCache ssmCache = new SSMCache(cache, expiration, true);

        List<SSMCache> ssmCaches = new ArrayList<>();
        ssmCaches.add(ssmCache);

        SSMCacheManager ssmCacheManager = new ExtendedSSMCacheManager();
        ssmCacheManager.setCaches(ssmCaches);

        return ssmCacheManager;
    }
}