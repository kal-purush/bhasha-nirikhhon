package com.ceos.spring_vote_21st.security.config;

import com.ceos.spring_vote_21st.member.domain.Role;
import com.ceos.spring_vote_21st.security.auth.application.filter.CustomAuthenticationFilter;
import com.ceos.spring_vote_21st.security.auth.application.filter.JwtAuthorizationFilter;
import com.ceos.spring_vote_21st.security.auth.application.jwt.JwtTokenProvider;
import com.ceos.spring_vote_21st.security.auth.application.jwt.refresh.RefreshTokenService;
import com.ceos.spring_vote_21st.security.handler.JwtAuthenticationFailureHandler;
import com.ceos.spring_vote_21st.security.handler.JwtAuthenticationSuccessHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;


@RequiredArgsConstructor
@Configuration
public class SecurityConfig {
    private final JwtTokenProvider jwtTokenProvider;
    private final UserDetailsService userDetailsService;
    private final JwtAuthenticationSuccessHandler jwtSuccessHandler;
    private final JwtAuthenticationFailureHandler jwtFailureHandler;
    private final RefreshTokenService refreshTokenService;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http, AuthenticationConfiguration authConfig) throws Exception {
        http
                .csrf(csrf -> csrf.disable())
                .httpBasic(httpBasic -> httpBasic.disable())
                .formLogin(formLogin -> formLogin.disable())
                .sessionManagement(session -> session.
                        sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(authorize -> authorize.
                                requestMatchers("/api/v1/users/signin", "/api/v1/users/signup", "/api/v1/users/logout","/health").permitAll()   // 인증 불필요
                                .requestMatchers("/api/v1/admin/**").hasRole(Role.ROLE_ADMIN.getKey())
                                .anyRequest().hasRole(Role.ROLE_USER.getKey())
//                        .anyRequest().authenticated()
                )
                .addFilterBefore(
                        new JwtAuthorizationFilter(jwtTokenProvider, userDetailsService, refreshTokenService),  // JWT 인가 필터 추가
                        UsernamePasswordAuthenticationFilter.class
                )
                .addFilterAt(customAuthenticationFilter(authConfig.getAuthenticationManager()), UsernamePasswordAuthenticationFilter.class)  // 자체 인증 필터 추가
        ;

        return http.build();
    }

    private CustomAuthenticationFilter customAuthenticationFilter(AuthenticationManager authenticationManager) {
        CustomAuthenticationFilter customAuthenticationFilter = new CustomAuthenticationFilter(authenticationManager, jwtSuccessHandler, jwtFailureHandler);
        customAuthenticationFilter.setFilterProcessesUrl("/api/v1/users/signin");

        return customAuthenticationFilter;
    }

    // 패스워드 인코더 등록
    @Bean
    public BCryptPasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    // AuthenticationManager Bean 등록
    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration configuration) throws Exception {
        return configuration.getAuthenticationManager();
    }
}