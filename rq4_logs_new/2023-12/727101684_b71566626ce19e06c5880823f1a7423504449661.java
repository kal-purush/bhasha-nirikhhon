package com.example.demo.filter;

import com.example.demo.auth.TokenProvider;
import com.example.demo.auth.TokenUserInfo;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import io.jsonwebtoken.Jwts;

// 클라이언트가 전송한 토큰을 검사하는 필터
@Component
@Slf4j
@RequiredArgsConstructor
public class JwtAuthFilter extends OncePerRequestFilter {

    private final TokenProvider tokenProvider;

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain)
            throws ServletException, IOException {

        try {
            String token = parseBearerToken(request); // 토큰 꺼냄
            log.info("JWT Token Filter is running... - token: {}", token);

            if(token != null && !token.equals("null")){

                TokenUserInfo userInfo // 토큰 안의 유저 정보 얻음
                        = tokenProvider.validateAndGetTokenUserInfo(token);

                // 인증 완료 처리
                // (스프링 시큐리티에게 인증정보를 전달해서 전역적으로 어플리케이션에서
                // 인증 정보를 활용할 수 있게 설정)
                AbstractAuthenticationToken auth
                        = new UsernamePasswordAuthenticationToken(
                        userInfo, // 컨트롤러에서 활용할 유저 정보
                        null // 인증된 사용자의 비밀번호 - 보통 null값
                ); // 생성자 호출!

                // 인증 완료 처리 시 클라이언트의 요청 정보 세팅
                auth.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

                // 스프링 시큐리티 컨테이너에 인증 정보 객체 등록
                SecurityContextHolder.getContext().setAuthentication(auth);


            } else {
                log.info("token이 null입니다!");
            }
        } catch (ExpiredJwtException e){
            log.warn("토큰의 기한이 만료되었습니다.");
            throw new JwtException("토큰 기한 만료!"); // 앞단에 끼워 놓은 필터에 전달 된다.

        } catch (Exception e) {
            e.printStackTrace();
            log.info("서명이 일치하지 않습니다! 토큰이 위조 되었습니다!");
        }


        filterChain.doFilter(request, response); // 컨트롤러로 요청이 간다.

    }


    private String parseBearerToken(HttpServletRequest request) {
        // 토큰 꺼내기
        String bearerToken = request.getHeader("Authorization");

        if(StringUtils.hasText(bearerToken)
                && bearerToken.startsWith("Bearer")) {
            return bearerToken.substring(7);
        }
        return null;


    }


}