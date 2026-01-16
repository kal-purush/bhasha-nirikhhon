package org.backend.global.filter;

import static org.backend.global.filter.MDCLogginKey.REQUEST_HTTP_METHOD;
import static org.backend.global.filter.MDCLogginKey.REQUEST_ID;
import static org.backend.global.filter.MDCLogginKey.REQUEST_IP;
import static org.backend.global.filter.MDCLogginKey.REQUEST_URI;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class MDCLogginFilter extends OncePerRequestFilter {

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        try {
            setMdc(request);
            filterChain.doFilter(request, response);
        } finally {
            MDC.clear();
        }
    }

    private void setMdc(final HttpServletRequest request) {
        MDC.put(REQUEST_ID.name(), UUID.randomUUID().toString());
        MDC.put(REQUEST_HTTP_METHOD.name(), request.getMethod());
        MDC.put(REQUEST_URI.name(), request.getRequestURI());
        MDC.put(REQUEST_IP.name(), request.getRemoteAddr());
    }
}