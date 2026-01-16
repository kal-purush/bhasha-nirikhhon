package com.ceos.spring_vote_21st.security.auth.application.jwt;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

/* JWT 설정
*
* */
// 스프링빈 등록: @EnableConfigurationProperties
// 환경변수 바인딩: @ConfigurationProperties
@Getter
@RequiredArgsConstructor
@ConfigurationProperties(prefix = "jwt")    //@EnableConfigurationPropeties로 스프링빈 등록
public class JwtProperties {

    private final String secret;
//    @Value("${jwt.access-token-expiration}")  => @ConfigurationProperties가 자동 바인딩
    private final long accessTokenExpiration;
//    @Value("${jwt.access-token-expiration}")
    private final long refreshTokenExpiration;

}