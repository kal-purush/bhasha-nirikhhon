package com.fatepet.petrest.common.config;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class SameSiteCookieFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletResponse httpServletResponse = (HttpServletResponse) response;

        HttpServletResponseWrapper responseWrapper = new HttpServletResponseWrapper(httpServletResponse) {
            @Override
            public void addHeader(String name, String value) {
                if ("Set-Cookie".equalsIgnoreCase(name)) {
                    // SameSite=None 추가
                    if (!value.toLowerCase().contains("samesite")) {
                        value += "; SameSite=None";
                    }
                    // Secure 누락 시 추가
                    if (!value.toLowerCase().contains("secure")) {
                        value += "; Secure";
                    }
                    // HttpOnly 누락 시 추가
                    if (!value.toLowerCase().contains("httponly")) {
                        value += "; HttpOnly";
                    }
                }
                super.addHeader(name, value);
            }
        };

        chain.doFilter(request, responseWrapper);
    }
}

