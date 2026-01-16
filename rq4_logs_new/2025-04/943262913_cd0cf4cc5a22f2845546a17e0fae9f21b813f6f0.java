package com.sign.controller.argumentresolver;

import com.sign.controller.CookieManager;
import com.sign.controller.ProtectedRequest;
import com.sign.controller.ResponseProtector;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.core.MethodParameter;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

@RequiredArgsConstructor
public class EmailTokenResolver implements HandlerMethodArgumentResolver {

    private static final String EMAIL_TOKEN_NAME = "email_token";

    private final ResponseProtector responseProtector;
    private final CookieManager cookieManager;

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.hasParameterAnnotation(ProtectedRequest.class)
                && String.class.isAssignableFrom(parameter.getParameterType());

    }

    @Override
    public Object resolveArgument(MethodParameter parameter, ModelAndViewContainer mavContainer,
                                  NativeWebRequest webRequest, WebDataBinderFactory binderFactory) throws Exception {
        HttpServletRequest request = webRequest.getNativeRequest(HttpServletRequest.class);
        String protectedEmailToken = cookieManager.findByName(request.getCookies(), EMAIL_TOKEN_NAME);
        return responseProtector.unpack(protectedEmailToken, String.class)
                .orElseThrow(() -> new IllegalArgumentException("이메일을 추출할 수 없습니다."));
    }
}