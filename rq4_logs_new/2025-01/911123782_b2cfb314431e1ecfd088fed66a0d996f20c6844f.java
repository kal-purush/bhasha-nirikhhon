package com.roomfit.be.auth.application;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@RequiredArgsConstructor
public class JwtTokenProvider {

    private final JwtProperties jwtProperties;  // JwtProperties를 주입받아서 사용

    // Access Token 생성
    public String generateAccessToken(String username) {
        return Jwts.builder()
                .setSubject(username)  // 사용자 이름을 subject로 설정
                .setIssuedAt(new Date())  // 발급 시간
                .setExpiration(new Date(System.currentTimeMillis() + jwtProperties.getAccessTokenExpirationTime()))  // 만료 시간
                .signWith(SignatureAlgorithm.HS256, jwtProperties.getSecretKey())  // 서명 (비밀 키 사용)
                .compact();  // JWT 생성
    }

    // Refresh Token 생성
    public String generateRefreshToken(String username) {
        return Jwts.builder()
                .setSubject(username)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + jwtProperties.getRefreshTokenExpirationTime()))
                .signWith(SignatureAlgorithm.HS256, jwtProperties.getSecretKey())
                .compact();
    }

    // JWT에서 Claims(사용자 정보 등) 추출
    public Claims getClaimsFromToken(String token) {
        return Jwts.parser()
                .setSigningKey(jwtProperties.getSecretKey())  // 서명 검증을 위한 키 설정
                .parseClaimsJws(token)
                .getBody();
    }

    // JWT에서 사용자 이름(Subject)을 추출
    public String getUsernameFromToken(String token) {
        Claims claims = getClaimsFromToken(token);
        return claims.getSubject();  // JWT에서 Subject는 보통 사용자 정보입니다.
    }

    // JWT가 만료되었는지 여부 확인
    public boolean isTokenExpired(String token) {
        Date expiration = getClaimsFromToken(token).getExpiration();
        return expiration.before(new Date());  // 현재 시간보다 만료 시간이 이전이면 expired
    }

    // JWT 토큰 검증 (서명 및 만료 시간 확인)
    public boolean validateToken(String token) {
        try {
            return !isTokenExpired(token);  // 만료된 토큰은 유효하지 않음
        } catch (Exception e) {
            return false;
        }
    }
}