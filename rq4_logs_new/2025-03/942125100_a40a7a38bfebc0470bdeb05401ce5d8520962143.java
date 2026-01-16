package com.securitydemo.filter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.io.IOException;
import java.util.Base64;

public class JwtAuthenticationFilter extends UsernamePasswordAuthenticationFilter {
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String authHeader = request.getHeader("Authorization");
        if (authHeader != null && authHeader.startsWith("Bearer ")) {
            String token = authHeader.substring(7);
            String decoded = new String(Base64.getDecoder().decode(token));
            if (decoded.contains("\"role\": \"USER\"")) {
                request.setAttribute("role", "ROLE_USER");
            } else if (decoded.contains("\"role\": \"ADMIN\"")) {
                request.setAttribute("role", "ROLE_ADMIN");
            }
        }
        chain.doFilter(request, response);
    }
}