package com.roomfit.be.auth.infrastructure.support;

import com.roomfit.be.auth.domain.AuthToken;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class AuthTokenRepositoryImpl implements AuthTokenRepository {

    private static final String KEY_PREFIX = "auth_refresh_token";
    private final RedisTemplate<String, AuthToken> redisTemplate;

    @Autowired
    public AuthTokenRepositoryImpl(@Qualifier("authTokenRedisTemplate") RedisTemplate<String, AuthToken> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public Optional<AuthToken> findByEmail(String email) {
        AuthToken authToken = (AuthToken) redisTemplate.opsForHash().get(KEY_PREFIX, email);
        return Optional.of(authToken);
    }

    @Override
    public void save(AuthToken authToken) {
        // Save the AuthToken in Redis with the email as the key
        redisTemplate.opsForHash().put(KEY_PREFIX, authToken.getEmail(), authToken);
    }

    @Override
    public void delete(String email) {
        // Delete the AuthToken based on the email
        redisTemplate.opsForHash().delete(KEY_PREFIX, email);
    }
}