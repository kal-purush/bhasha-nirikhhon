package com.bahubba.bahubbabookclub.service;

import com.bahubba.bahubbabookclub.model.entity.Reader;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseCookie;
import org.springframework.mock.web.MockHttpServletRequest;

import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the JwtService
 */
@SpringBootTest
public class JwtServiceTest {

    @Autowired
    JwtService jwtService;

    @Value("${app.properties.auth_cookie_name}")
    private String authCookieName;

    @Value("${app.properties.refresh_cookie_name}")
    private String refreshCookieName;

    @Value("${app.properties.secret_key}")
    private String secretKey;

    @Test
    void testGenerateJwtCookie() {
        ResponseCookie result = jwtService.generateJwtCookie(Reader.builder().username("name").build());

        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo(authCookieName);
        assertThat(result.toString().length()).isGreaterThan(0);
        assertThat(result.getPath()).isEqualTo("/api");
        assertThat(result.getMaxAge().getSeconds()).isEqualTo(24L * 60L * 60L);
        assertThat(result.isHttpOnly()).isTrue();
        assertThat(result.isSecure()).isTrue();
        assertThat(result.getDomain()).isNull();
        assertThat(result.getSameSite()).isEqualTo("None");
    }

    @Test
    void testGenerateJwtRefreshCookie() {
        ResponseCookie result = jwtService.generateJwtRefreshCookie("somecookie");

        assertThat(result).isNotNull();
        assertThat(result.getName()).isEqualTo(refreshCookieName);
        assertThat(result.toString().length()).isGreaterThan(0);
        assertThat(result.getPath()).isEqualTo("/api/v1/auth/refresh");
        assertThat(result.getMaxAge().getSeconds()).isEqualTo(24L * 60L * 60L);
        assertThat(result.isHttpOnly()).isTrue();
        assertThat(result.isSecure()).isTrue();
        assertThat(result.getDomain()).isNull();
        assertThat(result.getSameSite()).isEqualTo("None");
    }

    @Test
    void testGetJwtFromCookies() {
        MockHttpServletRequest mockReq = new MockHttpServletRequest();
        mockReq.setCookies(new Cookie(authCookieName, "foo"));

        String result = jwtService.getJwtFromCookies(mockReq);

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void testGetJwtRefreshFromCookies() {
        MockHttpServletRequest mockReq = new MockHttpServletRequest();
        mockReq.setCookies(new Cookie(refreshCookieName, "foo"));

        String result = jwtService.getJwtRefreshFromCookies(mockReq);

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("foo");
    }

    @Test
    void testExtractUsername() {
        String result = jwtService.extractUsername(
            Jwts.builder()
                .setSubject("someuser")
                .signWith(Keys.hmacShaKeyFor(Decoders.BASE64.decode(secretKey)))
                .compact()
        );

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("someuser");
    }

    @Test
    void testIsTokenValid() {
        boolean result = jwtService.isTokenValid(
            Jwts.builder()
                .setSubject("someuser")
                .setExpiration(new Date(System.currentTimeMillis() + 1000 * 60 * 60)) // 1 hr validity
                .signWith(Keys.hmacShaKeyFor(Decoders.BASE64.decode(secretKey)))
                .compact(),
            Reader.builder().username("someuser").build()
        );

        assertThat(result).isTrue();
    }

    @Test
    void testIsTokenValid_MismatchedName() {
        boolean result = jwtService.isTokenValid(
            Jwts.builder()
                .setSubject("someuser")
                .setExpiration(new Date(System.currentTimeMillis() + 1000 * 60 * 60))
                .signWith(Keys.hmacShaKeyFor(Decoders.BASE64.decode(secretKey)))
                .compact(),
            Reader.builder().username("someotheruser").build()
        );

        assertThat(result).isFalse();
    }

    @Test
    void testIsTokenValid_Expired() {
        // FIXME - Gracefully handle exception from expired token
    }
}