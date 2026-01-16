package com.spring.mood.projectmvc.interceptor;

import com.spring.mood.projectmvc.dto.responseDto.SignInUserInfoDTO;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

@Configuration
@RequiredArgsConstructor
public class AdminChangeInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 사용자의 역할을 세션이나 인증 객체에서 가져옵니다.
//        String userRole = (String) request.getSession().getAttribute("userRole");
        SignInUserInfoDTO user = (SignInUserInfoDTO) request.getSession().getAttribute("loginUser");

        // userRole 필드를 가져옵니다.
        String userRole = (user != null) ? user.getUserRole() : null;

        // 역할이 ADMIN이 아니면 접근을 막습니다.
        if (userRole == null || !userRole.equals("ADMIN")) {
            response.sendRedirect("/");
            return false;
        }

        // ADMIN 역할이라면 요청을 계속 처리합니다.
        return true;

    }
}