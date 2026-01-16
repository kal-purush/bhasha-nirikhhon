package cnsa.demo.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests((requests) -> requests
                        .requestMatchers("/public/**", "/login", "/").permitAll() // 공개적으로 접근 가능한 URL
                        .anyRequest().authenticated() // 나머지 모든 요청은 인증 필요
                )
                .formLogin((form) -> form
                        .loginPage("/login") // 사용자 정의 로그인 페이지
                        .permitAll()
                )
                .logout((logout) -> logout.permitAll());

        return http.build();
    }
}

