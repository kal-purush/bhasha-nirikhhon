package com.example.EnjoyTripBackend.util;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import static com.example.EnjoyTripBackend.util.SessionConst.LOGIN_MEMBER;

@Slf4j
public class SessionUserArgumentResolver implements HandlerMethodArgumentResolver {

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        boolean hasSessionUserAnnotation = parameter.hasParameterAnnotation(SessionUser.class);
        boolean hasStringType = parameter.getParameterType().equals(String.class);
        return hasSessionUserAnnotation && hasStringType;
    }

    @Override
    public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer, NativeWebRequest webRequest, WebDataBinderFactory binderFactory) throws Exception {
        HttpServletRequest httpRequest = (HttpServletRequest) webRequest.getNativeRequest();
        return (String) httpRequest.getSession().getAttribute(LOGIN_MEMBER);
    }
}