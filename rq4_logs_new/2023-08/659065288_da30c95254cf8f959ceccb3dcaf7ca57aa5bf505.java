package kr.rogarithm.todos.global.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import java.io.IOException;
import java.util.List;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.server.ResponseStatusException;

@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private final JwtAuthenticationManager jwtAuthenticationManager;

    private final List<String> excludedPaths = List.of(
            "/auth/login", "/verify/account", "/verify/crn", "/verify/nickname", "/user"
    );

    public JwtAuthenticationFilter(JwtAuthenticationManager jwtAuthenticationManager) {
        this.jwtAuthenticationManager = jwtAuthenticationManager;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        if (excludedPaths.contains(request.getServletPath())) {
            filterChain.doFilter(request, response);
            return;
        }

        String authHeader = request.getHeader("Authorization");

        if (authHeader == null || !authHeader.startsWith("Bearer ")) {

            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding("UTF-8");

            response.getWriter().println("{ \"message\": \"" + "인증을 위해 JWT 토큰이 필요합니다." + "\" }");

            return;
        }

        String token = authHeader.substring("Bearer ".length());

        try {
            jwtAuthenticationManager.verifyToken(token);
        } catch (ExpiredJwtException e) {

            ObjectMapper mapper = new ObjectMapper();

            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding("UTF-8");

            ResponseStatusException responseStatusException = new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "토큰이 만료되었습니다.");

            mapper.writeValue(response.getWriter(), responseStatusException);

        } catch (JwtException e) {

            ObjectMapper mapper = new ObjectMapper();

            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            response.setCharacterEncoding("UTF-8");

            ResponseStatusException responseStatusException = new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED, "잘못된 토큰 형식입니다.");

            mapper.writeValue(response.getWriter(), responseStatusException);

        }

        filterChain.doFilter(request, response);
    }
}