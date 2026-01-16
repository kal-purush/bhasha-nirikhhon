package com.group_component.master_gateway.filters;

import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;

import java.io.IOException;

public class RemoveDuplicateHeaders implements Filter {

    @Override
    public void doFilter(ServletRequest servletRequest,
                         ServletResponse servletResponse,
                         FilterChain filterChain) throws IOException, ServletException {
        HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;

        // Wrap the response
        HttpServletResponseWrapper responseWrapper = new HttpServletResponseWrapper(httpResponse) {
            @Override
            public void addHeader(String name, String value) {
                if ("Access-Control-Allow-Origin".equalsIgnoreCase(name)) {
                    // Ensure this header is set only once
                    if (getHeader(name) == null) {
                        super.addHeader(name, value);
                    }
                } else {
                    super.addHeader(name, value);
                }
            }

            @Override
            public void setHeader(String name, String value) {
                if ("Access-Control-Allow-Origin".equalsIgnoreCase(name)) {
                    super.setHeader(name, value); // Replace any existing value
                } else {
                    super.setHeader(name, value);
                }
            }
        };

        // Proceed with the wrapped response
        filterChain.doFilter(servletRequest, responseWrapper);
    }
}