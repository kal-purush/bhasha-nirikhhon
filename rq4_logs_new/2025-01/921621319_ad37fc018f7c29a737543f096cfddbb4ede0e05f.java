package com.metamong.mt.global.commons;

import java.io.IOException;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;

public class HttpMethodOverrideFilter implements Filter {

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest httpRequest = (HttpServletRequest) request;
		String methodOverride = httpRequest.getHeader("X-HTTP-Method-Override");
		
		if (methodOverride != null && !methodOverride.isEmpty()) {
			HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper(httpRequest) {
				@Override
				public String getMethod() {
					return methodOverride;
				}
			};
			chain.doFilter(wrapper, response);
		} else {
			chain.doFilter(request, response);
		}
	}
}