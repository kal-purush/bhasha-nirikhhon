package com.rljj.chipservice.global.config.jwt;

import com.rljj.switchswitchcommon.jwt.JwtProvider;
import com.rljj.switchswitchcommon.jwt.JwtProviderImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JwtConfig {

    @Value("${jwt.expired.access-token}")
    private long accessTokenExpireTime;

    @Value("${jwt.expired.refresh-token}")
    private long refreshTokenExpireTime;

    @Value("${jwt.secret}")
    private String jwtSecret;

    @Bean
    public JwtProvider jwtProvider() {
        return new JwtProviderImpl(accessTokenExpireTime, refreshTokenExpireTime, jwtSecret);
    }
}