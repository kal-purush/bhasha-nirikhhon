package com.ceos.spring_vote_21st.security.auth.application.filter;

import com.ceos.spring_vote_21st.global.error.CustomException;
import com.ceos.spring_vote_21st.global.error.ErrorCode;
import com.ceos.spring_vote_21st.security.auth.application.jwt.JwtTokenProvider;
import com.ceos.spring_vote_21st.security.auth.application.jwt.refresh.RefreshTokenService;
import com.ceos.spring_vote_21st.security.auth.user.detail.CustomUserDetails;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.WebUtils;

import java.io.IOException;

/**
* 역할 정리
HTTP 요청의 Authorization 헤더에서 JWT 토큰 추출
토큰의 유효성 검증
유효하다면, Spring Security 인증 객체(SecurityContext) 에 등록
유효하지 않으면 인증 없이 다음 필터로 넘김 (혹은 예외 처리)
* */


// JWT 토큰 검증 및 인증처리
@Slf4j
@RequiredArgsConstructor
public class JwtAuthorizationFilter extends OncePerRequestFilter {
    private final JwtTokenProvider jwtTokenProvider;
    private final UserDetailsService userDetailsService;
    private final RefreshTokenService refreshTokenService;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        // 헤더 Authorization 필드에서 토큰 추출
        String accessToken = getTokenFromRequest(request);

        // token 검증 (+ access token 재발행)
        validateAndReissue(request, response, accessToken);

        // 인증정보 저장
        String username = jwtTokenProvider.getUsernameFromToken(accessToken);
        CustomUserDetails customUserDetails = (CustomUserDetails) userDetailsService.loadUserByUsername(username);
        // 1. 인증 객체 생성: Principal로 UserDetails넣음
        // 나중에 UserDetails를 꺼낼 때: SecurityContextHolder.getContext().getAuthentication().getPrincipal()
        UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(customUserDetails, null, customUserDetails.getAuthorities());
        authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

        // 2. SecurityContext 컨텍스트에 인증정보 저장
        SecurityContextHolder.getContext().setAuthentication(authentication);

        // 3. 다음 필터에게 request 전달
        filterChain.doFilter(request, response);
    }

    /**
     * 토큰 검증 및 만료시 재발행
     * */
    private void validateAndReissue(HttpServletRequest request, HttpServletResponse response, String accessToken) {
        if (!jwtTokenProvider.validateToken(accessToken)) { // access 토큰이 만료된거면..
            log.info("Token Expired: 토큰 재발행 시작");
            Cookie refreshCookie = WebUtils.getCookie(request, "refreshToken");
            String refreshToken;

            if (refreshCookie != null) {
                refreshToken = refreshCookie.getValue();
                // 1.refreshToken 서명 검증
                if(!jwtTokenProvider.validateToken(refreshToken)) throw new CustomException(ErrorCode.INVALID_REFRESH_TOKEN);

                // 2. 본인 refresh token인지 검증
                jwtTokenProvider.validateTokenOwnership(accessToken, refreshToken);
                Long userId = jwtTokenProvider.getUserIdFromToken(accessToken);

                String newAccessToken = jwtTokenProvider.generateAccessToken(userId, jwtTokenProvider.getUsernameFromToken(accessToken));

                // 3. 응답 헤더에 새 access token 담기
                response.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + newAccessToken);

                log.info("access Token 재발급 성공");
            }
        }
    }

    // 헤더에서 "Authentication" 필드의 Bearer 토큰 추출
    private String getTokenFromRequest(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        if (bearerToken != null && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7);    //"Bearer "이후 추출
        }
        return null;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) throws ServletException {
        String requestURI = request.getRequestURI();

        return requestURI.startsWith("/api/v1/users/signup") || requestURI.equals("/api/v1/users/signin") || requestURI.equals("/api/v1/users/logout") || requestURI.equals("/api/v1/health");
    }

}