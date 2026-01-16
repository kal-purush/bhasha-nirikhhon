package com.ceos.spring_vote_21st.config;

import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(auth -> auth
                        // h2-console 전체 허용
                        .requestMatchers("/h2-console/**","/health").permitAll()
                        // 그 외는 인증 필요
                        .anyRequest().authenticated()
                )
                // h2-console iframe 접근 허용 (X-Frame-Options 해제)
                .headers(headers -> headers.disable())
                // h2-console은 CSRF 예외처리
                .csrf(csrf -> csrf.ignoringRequestMatchers("/h2-console/**"));

        // 로그인 폼은 Spring Security 기본 폼 사용
        return http.build();
    }
}