package local.socialnetwork.userservice.filter;

import io.jsonwebtoken.MalformedJwtException;

import io.jsonwebtoken.security.SignatureException;

import jakarta.servlet.FilterChain;

import jakarta.servlet.http.HttpFilter;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import local.socialnetwork.userservice.util.JwtUtil;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import lombok.experimental.FieldDefaults;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class JwtVerificationTokenFilter extends HttpFilter {

    final JwtUtil jwtUtil;

    @Override
    protected void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain) throws IOException {

        if (isAuthMissing(request)) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication information is missing.");
            return;
        }

        String bearerToken = getBearerToken(request);

        if (bearerToken == null) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Bearer token is missing.");
            return;
        }

        try {
            if (!jwtUtil.isTokenExpired(bearerToken)) {
                chain.doFilter(request, response);
            } else {
                response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "JWT token is expired.");
            }
        } catch (SignatureException e) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Invalid JWT signature.");
        } catch (MalformedJwtException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "JWT token is malformed.");
        } catch (Exception e) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "Authentication failed due to an internal error. Please try again.");
        }
    }

    private boolean isAuthMissing(HttpServletRequest request) {
        return request.getHeader("Authorization") == null;
    }

    private String getBearerToken(HttpServletRequest request) {
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            return authHeader.substring(7);
        }
        return null;
    }
}