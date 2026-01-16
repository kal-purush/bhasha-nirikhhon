package com.ceos.spring_vote_21st.security.auth.application.filter;

import com.ceos.spring_vote_21st.security.auth.web.dto.SignInDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.authentication.AuthenticationFailureHandler;
import org.springframework.security.web.authentication.AuthenticationSuccessHandler;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.io.IOException;

public class CustomAuthenticationFilter extends UsernamePasswordAuthenticationFilter {

    public CustomAuthenticationFilter(AuthenticationManager authenticationManager,
                                      AuthenticationSuccessHandler success,
                                      AuthenticationFailureHandler failure) {
        super(authenticationManager);
        setAuthenticationSuccessHandler(success);
        setAuthenticationFailureHandler(failure);
    }

    @Override
    public Authentication attemptAuthentication(HttpServletRequest req,
                                                HttpServletResponse res) throws AuthenticationException {
        SignInDTO dto = null;
        try {
            dto = new ObjectMapper()
                    .readValue(req.getInputStream(), SignInDTO.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        UsernamePasswordAuthenticationToken authentication = new UsernamePasswordAuthenticationToken(dto.getUsername(), dto.getPassword());
        return this.getAuthenticationManager().authenticate(authentication);
    }
}