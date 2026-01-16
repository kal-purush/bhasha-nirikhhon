package org.springbootdeveloper2.config;

import lombok.RequiredArgsConstructor;
import org.springbootdeveloper2.service.UserDetailService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

@Configuration
@EnableWebSecurity
@RequiredArgsConstructor
public class WebSecurityConfig {

    private final UserDetailService userSerivce;

    // 1. 스프링 시큐리티 기능 비활성화 = 모든 곳에 인증, 인가 서비스를 적용하지 않게 설정
    // 정적 리소스, h2의 데이터 확인에는 인증, 인가 서비스 적용 x
    @Bean
    public WebSecurityCustomizer configure() {
        return (web) -> web.ignoring()
                .requestMatchers(new AntPathRequestMatcher("/static/**"));
    }

    // 2. 특정 HTTP 요청에 대한 웹 기반 보안 구성
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        return http
                .authorizeHttpRequests(auth -> auth // 3. 인증, 인가 설정
                        .requestMatchers( // 특정 요청과 일치하는 url에 대한 액세스 설정
                                new AntPathRequestMatcher("/login"),
                                new AntPathRequestMatcher("/signup"),
                                new AntPathRequestMatcher("/user")
                        ).permitAll() // login ... 요청이 오면 인증, 인가 없이도 누구나 접근 가능
                        .anyRequest().authenticated()) // 위에서 설정한 url 이외의 요청에 대해.authenticated() = 별도의 인가는 x, 인증이 성공된 상태여야 함
                .formLogin(formLogin -> formLogin // 4. 폼 기반 로그인 url 설정
                        .loginPage("/login")
                        .defaultSuccessUrl("/articles")
                )
                .logout(logout -> logout // 5. 로그아웃 url 설정
                        .logoutSuccessUrl("/login")
                        .invalidateHttpSession(true) // 로그아웃 이후 세션을 전체 삭제할지 여부 설정
                )
                .csrf(AbstractHttpConfigurer::disable) // 6. csrf 비활성화
                .build();
    }

    // 7. 인증 관리자 관련 설정
    // 사용자 정보를 가져올 서비스를 재정의, 인증 방법을 설정할 때 사용
    @Bean
    public AuthenticationManager authenticationManager(BCryptPasswordEncoder bCryptPasswordEncoder) {
        DaoAuthenticationProvider authProvider = new DaoAuthenticationProvider();
        authProvider.setUserDetailsService(userSerivce); // 8. 사용자 정보 서비스 설정
        authProvider.setPasswordEncoder(bCryptPasswordEncoder);
        return new ProviderManager(authProvider);
    }

    // 9. 패스워드 인코더로 사용할 빈 등록
    @Bean
    public BCryptPasswordEncoder bCryptPasswordEncoder() {
        return new BCryptPasswordEncoder();
    }

}