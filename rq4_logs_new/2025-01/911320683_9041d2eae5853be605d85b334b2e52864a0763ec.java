package com.sofkau.bank.http.converters;

import com.sofkau.bank.entities.Client;
import com.sofkau.bank.entities.User;
import com.sofkau.bank.http.tokens.JwtToken;
import com.sofkau.bank.utils.jwt.JwtInterpreter;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.CredentialsExpiredException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.web.authentication.AuthenticationConverter;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class JwtAuthenticationConverter implements AuthenticationConverter {
    private final JwtInterpreter jwtInterpreter;

    public JwtAuthenticationConverter(JwtInterpreter jwtInterpreter) {
        this.jwtInterpreter = jwtInterpreter;
    }

    @Override
    public Authentication convert(HttpServletRequest request) {
        String authHeader = request.getHeader(HttpHeaders.AUTHORIZATION) != null
                ? request.getHeader(HttpHeaders.AUTHORIZATION).trim()
                : null;

        if (authHeader == null || !StringUtils.startsWithIgnoreCase(authHeader, "Bearer"))
            return null;

        String jwtToken = authHeader.substring(7).trim();

        if (jwtToken.equalsIgnoreCase("Bearer"))
            throw new BadCredentialsException("Empty bearer authentication token");

        String userEmail = jwtInterpreter.extractUserEmail(jwtToken);

        User user = User.builder().email(userEmail).build();
        UserDetails client = Client.from(user).build();

        if (!jwtInterpreter.isTokenValid(jwtToken, client))
            throw new BadCredentialsException("Invalid bearer authentication token");

        JwtToken authentication = JwtToken.create(userEmail, jwtToken);
        authentication.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));

        return authentication;
    }
}