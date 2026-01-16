package com.gxdxx.instagram.config.jwt;

import com.gxdxx.instagram.entity.User;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;
import java.util.Set;

// 토큰을 생성하고 올바른 토큰인지 유효성 검사를 하고, 토큰에서 필요한 정보를 가져오는 클래스
@RequiredArgsConstructor
@Service
public class TokenProvider {

    private final JwtProperties jwtProperties;
    private static final long ACCESS_TIME =  30 * 60 * 1000L;   // 30분
    private static final long REFRESH_TIME =  14 * 24 * 60 * 60 * 1000L;    // 2주
    public static final String ACCESS_TOKEN = "access_token";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String TOKEN_PREFIX = "Bearer ";

    // 토큰 생성 메서드
    public String createToken(User user, String type) {
        Date now = new Date();
        Date expiry = new Date(now.getTime() + (type.equals(ACCESS_TOKEN) ? ACCESS_TIME : REFRESH_TIME));
        String subject = type.equals(ACCESS_TOKEN) ? user.getNickname() : REFRESH_TOKEN;

        return Jwts.builder()
                .setHeaderParam(Header.TYPE, Header.JWT_TYPE)   // 헤더 typ: jwt
                .setIssuer(jwtProperties.getIssuer())   // 내용 iss: yml 파일에서 설정한 값
                .setIssuedAt(now)   // 내용 iat: 현재 시간
                .setExpiration(expiry)  // 내용 exp: 만료일자
                .setSubject(subject)    // 내용 sub: 토큰 제목
                .claim("id", user.getId())  // 클레임 id: 유저 id, 비공개 클레임(공개되면 안되는 클레임)
                .signWith(SignatureAlgorithm.HS256, jwtProperties.getSecretKey())   // 서명: 비밀값과 함께 해시값을 HS256 방식으로 암호화
                .compact();
    }

    // JWT 토큰 유효성 검증 메서드
    public boolean validateToken(String token) {
        try {
            Jwts.parser()
                    .setSigningKey(jwtProperties.getSecretKey())    // 비밀값으로 토큰을 복호화
                    .parseClaimsJws(token);
            return true;
        } catch (Exception e) { // 복호화 과정에서 에러가 나면 유효하지 않은 토큰
            return false;
        }
    }

    // 토큰 기반으로 인증 정보를 가져오는 메서드
    public Authentication getAuthentication(String token) {
        Claims claims = getClaims(token);
        Set<SimpleGrantedAuthority> authorities = Collections.singleton(new SimpleGrantedAuthority("ROLE_USER"));

        return new UsernamePasswordAuthenticationToken(
                new org.springframework.security.core.userdetails.User(claims.getSubject(), "", authorities),
                token,
                authorities
        );
    }

    // 토큰의 sub에 refresh_token이 담겨있을 경우, 인증 실패
    public boolean isAccessToken(String token) {
        if (getSub(token).equals(ACCESS_TOKEN)) {
            return true;
        }
        return false;
    }

    // 토큰 기반으로 sub를 가져오는 메서드
    private String getSub(String token) {
        Claims claims = getClaims(token);
        return claims.getSubject();
    }

    // 토큰 기반으로 회원 id를 가져오는 메서드
    public Long getUserId(String token) {
        Claims claims = getClaims(token);
        return claims.get("id", Long.class);
    }

    // 비밀값으로 토큰을 복호화한 뒤 클레임을 가져오는 메서드
    private Claims getClaims(String token) {
        return Jwts.parser()
                .setSigningKey(jwtProperties.getSecretKey())
                .parseClaimsJws(token)
                .getBody();
    }

    public void addCookie(HttpServletResponse response, String refreshToken) {
        Cookie cookie = createCookie(refreshToken);
        response.addCookie(cookie);
    }

    public Cookie createCookie(String refreshToken) {
        String cookieName = REFRESH_TOKEN;
        String cookieValue = refreshToken;
        Cookie cookie = new Cookie(cookieName, cookieValue);

        // 쿠키 속성 설정
        cookie.setHttpOnly(true);  //httponly 옵션 설정
        cookie.setSecure(true); //https 옵션 설정
        cookie.setPath("/"); // 모든 곳에서 쿠키열람이 가능하도록 설정
        cookie.setMaxAge(60 * 60 * 24); //쿠키 만료시간 설정

        return cookie;
    }

    public void setHeader(HttpServletResponse response, String accessToken) {
        response.addHeader(HEADER_AUTHORIZATION, TOKEN_PREFIX + accessToken);
    }

}