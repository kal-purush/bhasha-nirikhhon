package moofarm.ssuckssuck.global.security;

import io.jsonwebtoken.Claims;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import moofarm.ssuckssuck.global.exception.ExpiredTokenException;
import moofarm.ssuckssuck.global.exception.InvalidTokenException;
import moofarm.ssuckssuck.global.exception.RefreshTokenExpiredException;
import moofarm.ssuckssuck.global.property.JwtProperties;
import moofarm.ssuckssuck.global.security.auth.AuthDetails;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Date;

@Slf4j
@Component
@RequiredArgsConstructor
public class JwtTokenProvider {

    private final JwtProperties jwtProperties;
    private final String ACCESS_TOKEN = "access_token";
    private final String REFRESH_TOKEN = "refresh_token";
    private final String TYPE = "type";
    private final String ISSUER = "moofarm";

    public String resolveToken(HttpServletRequest request) {
        String rawHeader = request.getHeader(jwtProperties.getHeader());

        if (rawHeader != null
                && rawHeader.length() > jwtProperties.getPrefix().length()
                && rawHeader.startsWith(jwtProperties.getPrefix())) {
            return rawHeader.substring(jwtProperties.getPrefix().length() + 1);
        }
        return null;
    }

    public String resolveAccessToken(HttpServletRequest request) {
        return resolveCookie(request, "SSUCK_TOKEN");
    }

    public String resolveRefreshToken(HttpServletRequest request) {
        return resolveCookie(request, "SSUCK_REFRESH_TOKEN");
    }

    private String resolveCookie(HttpServletRequest request, String cookieName) {
        Cookie[] cookies = request.getCookies();

        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName().equals(cookieName)) {
                    return cookie.getValue();
                }
            }
        }

        return null;
    }

    public Authentication getAuthentication(String token) {
        String id = getJws(token).getBody().getSubject();
        UserDetails userDetails = new AuthDetails(id);
        return new UsernamePasswordAuthenticationToken(
                userDetails, "", null);
    }

    private Jws<Claims> getJws(String token) {
        try {
            return Jwts.parserBuilder().setSigningKey(getSecretKey()).build().parseClaimsJws(token);
        } catch (ExpiredJwtException e) {
            throw ExpiredTokenException.EXCEPTION;
        } catch (Exception e) {
            throw InvalidTokenException.EXCEPTION;
        }
    }

    private Key getSecretKey() {
        return Keys.hmacShaKeyFor(jwtProperties.getSecretKey().getBytes(StandardCharsets.UTF_8));
    }

    private String buildAccessToken(
            Long id, Date issuedAt, Date accessTokenExpiresIn) {
        final Key encodedKey = getSecretKey();
        return Jwts.builder()
                .setIssuer(ISSUER)
                .setIssuedAt(issuedAt)
                .setSubject(id.toString())
                .claim(TYPE, ACCESS_TOKEN)
                .setExpiration(accessTokenExpiresIn)
                .signWith(encodedKey)
                .compact();
    }

    private String buildRefreshToken(Long id, Date issuedAt, Date accessTokenExpiresIn) {
        final Key encodedKey = getSecretKey();
        return Jwts.builder()
                .setIssuer(ISSUER)
                .setIssuedAt(issuedAt)
                .setSubject(id.toString())
                .claim(TYPE, REFRESH_TOKEN)
                .setExpiration(accessTokenExpiresIn)
                .signWith(encodedKey)
                .compact();
    }

    public String generateAccessToken(Long id) {
        final Date issuedAt = new Date();
        final Date accessTokenExpiresIn =
                new Date(issuedAt.getTime() + jwtProperties.getAccessExp() * 1000);

        return buildAccessToken(id, issuedAt, accessTokenExpiresIn);
    }

    public String generateRefreshToken(Long id) {
        final Date issuedAt = new Date();
        final Date refreshTokenExpiresIn =
                new Date(issuedAt.getTime() + jwtProperties.getRefreshExp() * 1000);
        return buildRefreshToken(id, issuedAt, refreshTokenExpiresIn);
    }

    public boolean isAccessToken(String token) {
        return getJws(token).getBody().get(TYPE).equals(ACCESS_TOKEN);
    }

    public boolean isRefreshToken(String token) {
        return getJws(token).getBody().get(TYPE).equals(REFRESH_TOKEN);
    }

    public Long parseAccessToken(String token) {
        if (isAccessToken(token)) {
            Claims claims = getJws(token).getBody();
            return Long.parseLong(claims.getSubject());
        }
        throw InvalidTokenException.EXCEPTION;
    }

    public Long parseRefreshToken(String token) {
        try {
            if (isRefreshToken(token)) {
                Claims claims = getJws(token).getBody();
                return Long.parseLong(claims.getSubject());
            }
        } catch (ExpiredTokenException e) {
            throw RefreshTokenExpiredException.EXCEPTION;
        }
        throw InvalidTokenException.EXCEPTION;
    }

    public void setHeaderAccessToken(HttpServletResponse response, String accessToken) {
        Cookie accessTokenCookie = new Cookie("SSUCK_TOKEN", accessToken);
        accessTokenCookie.setMaxAge(86400);
        accessTokenCookie.setPath("/");
        accessTokenCookie.setHttpOnly(true);

        response.addCookie(accessTokenCookie);
    }
    public void setHeaderAccessTokenEmpty(HttpServletResponse response) {
        Cookie accessTokenCookie = new Cookie("SSUCK_TOKEN", "");
        accessTokenCookie.setMaxAge(0);
        accessTokenCookie.setPath("/");
        accessTokenCookie.setHttpOnly(true);

        response.addCookie(accessTokenCookie);
    }

    // RefreshToken 헤더 설정
    public void setHeaderRefreshToken(HttpServletResponse response, String refreshToken) {
        Cookie refreshTokenCookie = new Cookie("SSUCK_REFRESH_TOKEN", refreshToken);
        refreshTokenCookie.setMaxAge(604800);
        refreshTokenCookie.setPath("/");
        refreshTokenCookie.setHttpOnly(true);

        response.addCookie(refreshTokenCookie);
    }
    public void setHeaderRefreshTokenEmpty(HttpServletResponse response) {
        Cookie refreshTokenCookie = new Cookie("SSUCK_REFRESH_TOKEN", "");
        refreshTokenCookie.setMaxAge(0);
        refreshTokenCookie.setPath("/");
        refreshTokenCookie.setHttpOnly(true);

        response.addCookie(refreshTokenCookie);
    }

    public Long getRefreshTokenTTlSecond() {
        return jwtProperties.getRefreshExp();
    }
}