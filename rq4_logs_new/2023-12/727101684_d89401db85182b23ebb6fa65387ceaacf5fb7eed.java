package com.example.demo.filter;


import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.JwtException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
@Slf4j
public class JwtExceptionFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        try {
            // 예외가 발생하지 않으면 Auth Filter로 통과~
            filterChain.doFilter(request, response);
        } catch (JwtException e) {
            // 토큰이 만료되었을 시 Auth Filter에서 예외를 강제 발생 -> 앞에 있는 Exception Filter로 전달
            log.info("만료 예외 발생! - {}", e.getMessage());
            setErrorResponse(response, e);
        }
    }

    private void setErrorResponse(HttpServletResponse response, JwtException e) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED); // 에러 번호가 무엇인지 모를때
        response.setContentType("application/json; charset=UTF-8");

        // Map 생성 및 데이터 추가
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("message", e.getMessage());
        responseMap.put("code", 401);

        // Map을 JSON 문자열로 변환
        String jsonString = new ObjectMapper().writeValueAsString(responseMap);

        // JSON 데이터를 응답객체에 실어서 브라우저로 바로 응답.
        response.getWriter().write(jsonString);


    }

}