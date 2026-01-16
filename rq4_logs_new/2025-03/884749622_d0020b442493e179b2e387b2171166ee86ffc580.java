package se.digg.eudiw.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.*;
import org.springframework.util.MultiValueMap;
import se.digg.eudiw.model.credentialissuer.CredentialOfferParam;

import java.time.Duration;

@Configuration
public class ValKeyConfig {
    @Bean
    RedisTemplate<String, MultiValueMap<String, String>> valKeyMultiValueMapTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, MultiValueMap<String, String>> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        return template;
    }

    @Bean
    RedisTemplate<String, CredentialOfferParam> valKeyCredentialOfferParamRedisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, CredentialOfferParam> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);
        return template;
    }
}

