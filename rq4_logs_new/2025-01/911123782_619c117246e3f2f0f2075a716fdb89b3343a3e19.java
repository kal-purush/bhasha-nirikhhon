package com.roomfit.be.auth.infrastructure;

import com.roomfit.be.auth.domain.RefreshToken;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Repository
public class RefreshTokenRepositoryImpl implements RefreshTokenRepository {

    private static final String KEY_PREFIX = "auth_refresh_token:";
    private final RedisTemplate<String, RefreshToken> redisTemplate;

    @Autowired
    public RefreshTokenRepositoryImpl(@Qualifier("authTokenRedisTemplate") RedisTemplate<String, RefreshToken> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public Optional<RefreshToken> findByEmail(String email) {
        RefreshToken refreshToken = redisTemplate.opsForValue().get(KEY_PREFIX+email);
        return Optional.ofNullable(refreshToken);
    }

    @Override
    public void save(RefreshToken refreshToken) {
        String redisKey = KEY_PREFIX + refreshToken.getEmail();
        redisTemplate.opsForValue().set(redisKey, refreshToken);
        redisTemplate.expire(redisKey, refreshToken.getExpiration(), TimeUnit.HOURS);
    }



    @Override
    public void delete(String email) {
        redisTemplate.delete(KEY_PREFIX+ email);
    }
}