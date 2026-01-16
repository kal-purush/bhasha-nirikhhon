package com.chapssal_tteok.preview.security;

import com.chapssal_tteok.preview.security.jwt.JwtAccessDeniedHandler;
import com.chapssal_tteok.preview.security.jwt.JwtAuthenticationEntryPoint;
import com.chapssal_tteok.preview.security.jwt.JwtAuthenticationFilter;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

@Configuration
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtAccessDeniedHandler jwtAccessDeniedHandler;
    private final JwtAuthenticationEntryPoint jwtAuthenticationEntryPoint;
    private final JwtAuthenticationFilter jwtAuthenticationFilter;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .exceptionHandling((exceptionHandling) -> exceptionHandling
                        .accessDeniedHandler(jwtAccessDeniedHandler) // 인증은 되었지만 권한이 부족할 때
                        .authenticationEntryPoint(jwtAuthenticationEntryPoint)) // 인증에 실패했을 때
                // disable session management
                .sessionManagement((sessionManagement) -> sessionManagement
                        .sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                // 폼 기반 로그인 설정
                .formLogin(formLogin -> formLogin
                        .loginPage("/login")
                        .defaultSuccessUrl("/")
                )
                .logout(logout -> logout
                        .logoutSuccessUrl("/")
                        .invalidateHttpSession(true))
                // 경로 접속 권한 설정
                .authorizeHttpRequests((authorizeRequests) -> authorizeRequests
                        // Swagger, login, register, refresh 허용
                        .requestMatchers("/","/swagger-ui/**","/v3/api-docs/**").permitAll()
                        .requestMatchers("/api/auth/register",
                                "/api/auth/login",
                                "/api/auth/refresh",
                                "/api/auth/users/isunique/**",
                                "/api/auth/mail").permitAll()
                        .anyRequest().authenticated())
                // UsernamePasswordAuthenticationFilter 전에 JwtAuthenticationFilter 추가
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);


        return http.build();
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration configuration) throws Exception {
        return configuration.getAuthenticationManager();
    }
}