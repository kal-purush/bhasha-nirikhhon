package com.gxdxx.instagram.config;

import com.gxdxx.instagram.config.jwt.TokenProvider;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@RequiredArgsConstructor
public class TokenAuthenticationFilter extends OncePerRequestFilter {

    private final TokenProvider tokenProvider;

    // Access Token 값이 담긴 Authorization 헤더값을 가져온 뒤 Access Token이 유효하면 인증 정보를 설정해줌
    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {

        // 요청 헤더의 Authorization 키의 값 조회
        String authorizationHeader = request.getHeader(TokenProvider.HEADER_AUTHORIZATION);
        // 가져온 값에서 접두사 제거
        String token = getAccessToken(authorizationHeader);
        // 가져온 토큰이 유효한지 확인하고, 유효할 때는 시큐리티 컨텍스트에 인증 정보를 설정
        // 이후 컨텍스트 홀더에서 getAuthentication() 메서드를 사용해 인증 정보를 가져오면 회원 객체가 반환됨
        if (tokenProvider.validateToken(token) && tokenProvider.isAccessToken(token)) {
            Authentication authentication = tokenProvider.getAuthentication(token);
            SecurityContextHolder.getContext().setAuthentication(authentication);
        }

        filterChain.doFilter(request, response);
    }

    private String getAccessToken(String authorizationHeader) {
        if (authorizationHeader != null && authorizationHeader.startsWith(TokenProvider.TOKEN_PREFIX)) {
            return authorizationHeader.substring(TokenProvider.TOKEN_PREFIX.length());
        }
        return null;
    }

}