package com.example.demo.auth;

import com.example.demo.chatapi.util.SHA256;
import com.example.demo.userapi.entity.Role;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class TokenEmailCheckProvider {

    // 서명에 사용할 값 (512비트 이상의 랜덤 문자열)
    @Value("${jwt.secret}")
    private String SECRET_KEY;

    @Value("${jwt.passcode_salt}")
    private String PASSCODE_SALT;

    public String createToken(String email, String authcode, boolean isChecked) {
        // 토큰 만료 시간 생성
        Date expiry = Date.from(
                Instant.now().plus(1, ChronoUnit.DAYS)
        );


        // 추가 클레임 정의
        Map<String, String> claims = new HashMap<>();
        claims.put("authcode", SHA256.encrypt(PASSCODE_SALT + authcode));
        claims.put("isChecked", String.valueOf(isChecked));

        // 토큰 생성
        return Jwts.builder()
                // token header에 들어갈 서명
                .signWith(
                        Keys.hmacShaKeyFor(SECRET_KEY.getBytes()),
                        SignatureAlgorithm.HS512
                )
                // token payload에 들어갈 클레임 설정.
                .setClaims(claims) // 추가 클레임은 먼저 설정해야 함.
                .setIssuer("1nterface운영자") // iss: 발급자 정보
                .setIssuedAt(new Date()) // iat: 발급 시간
                .setExpiration(expiry) // exp: 만료 시간
                .setSubject(email) // sub: 토큰을 식별할 수 있는 주요 데이터
                .compact(); // String 리턴.
    }

    // 토큰 위조 여부 확인
    public TokenEmailCheckInfo validateAndGetTokenEmailCheckInfo(String token) {
        Claims claims = Jwts.parserBuilder()
                .setSigningKey(Keys.hmacShaKeyFor(SECRET_KEY.getBytes()))
                .build()
                .parseClaimsJws(token)
                .getBody();

        log.info("claims: {}", claims);

        return TokenEmailCheckInfo.builder()
                .email(claims.getSubject())
                .authCode(claims.get("authcode", String.class))
                .isChecked(Boolean.parseBoolean(claims.get("isChecked", String.class)))
                .build();
    }


    public boolean checkPasscode(String tokenAuthcode, String authcode) {
        return tokenAuthcode.equals(SHA256.encrypt(PASSCODE_SALT + authcode));

    }
}