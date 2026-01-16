package com.roomfit.be.auth.application.resolver;

import com.roomfit.be.auth.application.domain.JwtTokenExtractor;
import com.roomfit.be.auth.application.domain.JwtTokenProvider;
import com.roomfit.be.auth.application.dto.UserDetails;
import com.roomfit.be.global.annontation.AuthCheck;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.MethodParameter;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;
@Component
@RequiredArgsConstructor
@Slf4j
public class AuthCheckResolver implements HandlerMethodArgumentResolver {

    private final JwtTokenProvider jwtTokenProvider;
    private final JwtTokenExtractor jwtTokenExtractor;

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getMethod().isAnnotationPresent(AuthCheck.class);
    }

    @Override
    public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer, NativeWebRequest webRequest, WebDataBinderFactory binderFactory) throws Exception {
        HttpServletRequest request = (HttpServletRequest) webRequest.getNativeRequest();
        String authorizationHeader = request.getHeader("Authorization");

        // Extract the token from the Authorization header
        String token = jwtTokenExtractor.extractTokenFromHeader(authorizationHeader);

        // Extract the role and other user details from the token
        UserDetails userDetails = jwtTokenProvider.extractUserDetailFromToken(token);

        // Retrieve the required role from the AuthCheck annotation
        AuthCheck authCheck = parameter.getMethodAnnotation(AuthCheck.class);
        String requiredRole = authCheck.role();
        log.info("Required Role: " + requiredRole);

        // Ensure the user has the correct role
        if (userDetails == null || !userDetails.getUserRole().equals(requiredRole)) {
            throw new IllegalAccessException("Access Denied: Insufficient Permissions");
        }

        return userDetails;
    }
}