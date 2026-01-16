
package com.lucamusic.user.config;

import com.lucamusic.user.utils.JwtUtil;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Utils
 * Clase de Filtro personalizado para la autentificación JWT
 * @version 1.0 Septiembre 2021
 * @author Julio
 */
@Slf4j
@Component
public class CustomJwtAuthenticationFilter extends OncePerRequestFilter {

    @Autowired
    private JwtUtil jwtTokenUtil;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
                    throws ServletException, IOException, java.io.IOException {

        try {
            // Recuperar el JWT token
            String jwtToken = extractJwtFromRequest(request);

            if (StringUtils.hasText(jwtToken) && jwtTokenUtil.validateToken(jwtToken)) {
                    UserDetails userDetails = new User(jwtTokenUtil.getUsernameFromToken(jwtToken), "",
                                    jwtTokenUtil.getRolesFromToken(jwtToken));

                    UsernamePasswordAuthenticationToken usernamePasswordAuthenticationToken = new UsernamePasswordAuthenticationToken(
                                    userDetails, null, userDetails.getAuthorities());

                    //Especificamos que el usuario está autenticado
                    SecurityContextHolder.getContext().setAuthentication(usernamePasswordAuthenticationToken);
            } else {
                    log.info("Cannot set the Security Context");
            }

        } catch (ExpiredJwtException | BadCredentialsException ex) {
            request.setAttribute("exception", ex);
            throw ex;
        }
        chain.doFilter(request, response);
    }

    private String extractJwtFromRequest(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        if (StringUtils.hasText(bearerToken) && bearerToken.startsWith("Bearer ")) {
                return bearerToken.substring(7, bearerToken.length());
        }
        return null;
    }

}