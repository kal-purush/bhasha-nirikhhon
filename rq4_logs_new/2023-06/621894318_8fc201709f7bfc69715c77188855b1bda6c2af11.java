package com.gxdxx.instagram.config;

import com.gxdxx.instagram.config.jwt.TokenProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.HttpStatusEntryPoint;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class SecurityConfig {

    private final TokenProvider tokenProvider;

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /**
     * 직접 정의한 필터(TokenAuthenticationFilter)에서 인증 작업을 진행하기 때문에
     * AuthenticationManager를 사용하지 않고 TokenProvider를 통해서 인증 후 SecurityContextHolder를 바로 사용
     */

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {

        http.cors();

        http
                .formLogin().disable()  // 자체 login으로 로그인을 진행하기 때문에, 기본 방식을 disable
                .httpBasic().disable()  // JWT 토큰을 사용한 로그인(Bearer 방식)이기 때문에 기본 설정인 httpBasic은 disable
                .csrf().disable();  // REST API를 사용하여 서버에 인증 정보를 저장하지 않고, 요청 시 인증 정보(JWT 토큰, OAuth2)를 담아서 요청하므로 보안 기능인 csrf가 필요가 없으므로 disable

        http.sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS);    // 세션을 사용하지 않기 때문에 세션 정책을 Stateless로 설정

        /**
         * 회원가입과 로그인을 위한 /api/users/signup, /api/users/login 로 들어오는 요청은 검증없이 요청을 허용하도록 설정
         */
        http.authorizeRequests()
                .requestMatchers("/api/users/signup", "/api/users/login", "/api/token").permitAll()
                .anyRequest().authenticated()
                .and().addFilterBefore(new TokenAuthenticationFilter(tokenProvider), UsernamePasswordAuthenticationFilter.class);

        // /api로 시작하는 url인 경우 인증 실패 시 401 Unauthorized를 반환하도록 예외 처리
        http.exceptionHandling()
                .defaultAuthenticationEntryPointFor(new HttpStatusEntryPoint(HttpStatus.UNAUTHORIZED),
                        new AntPathRequestMatcher("/api/**"));

        return http.build();

    }

}