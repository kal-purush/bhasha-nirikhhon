package km.cd.backend.challenge.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfiguration;
import org.springframework.security.config.annotation.web.configurers.CsrfConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.config.annotation.web.configurers.HttpBasicConfigurer;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity httpSecurity) throws Exception{
        httpSecurity
                .httpBasic(HttpBasicConfigurer::disable)
                .csrf(CsrfConfigurer::disable)
                .cors(Customizer.withDefaults())
                // h2 데이터베이스 접근을 위한 프레임 허용
                .headers(headersConfigurer ->
                        headersConfigurer.frameOptions(
                                HeadersConfigurer.FrameOptionsConfig::sameOrigin
                        ))

                .authorizeHttpRequests((authorize) ->
                        authorize
//                                .requestMatchers("/h2-console/**").authenticated() // h2 데이터베이스 콘솔 접근 허용
//                                .requestMatchers("/**").authenticated()
//                                .requestMatchers("/admin/**").hasRole("ADMIN") // admin으로 시작하는 uri는 관릴자 계정만 접근 가능
                                .anyRequest().permitAll()); //나머지 uri는 모든 접근 허용

        return httpSecurity.build();
    }
}