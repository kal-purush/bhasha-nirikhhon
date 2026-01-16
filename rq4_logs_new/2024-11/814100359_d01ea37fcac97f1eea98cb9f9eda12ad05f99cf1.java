package soma.edupiuser._config;


import static org.springframework.security.web.util.matcher.AntPathRequestMatcher.antMatcher;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import soma.edupiuser.oauth.HttpCookieOAuth2AuthorizationRequestRepository;
import soma.edupiuser.oauth.handler.OAuth2AuthenticationFailureHandler;
import soma.edupiuser.oauth.handler.OAuth2AuthenticationSuccessHandler;
import soma.edupiuser.oauth.jwt.JwtAuthorizationFilter;
import soma.edupiuser.oauth.service.CustomOAuth2UserService;

@RequiredArgsConstructor
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final CustomOAuth2UserService customOAuth2UserService;
    private final OAuth2AuthenticationSuccessHandler oAuth2AuthenticationSuccessHandler;
    private final OAuth2AuthenticationFailureHandler oAuth2AuthenticationFailureHandler;
    private final HttpCookieOAuth2AuthorizationRequestRepository httpCookieOAuth2AuthorizationRequestRepository;
    private final JwtAuthorizationFilter jwtAuthorizationFilter;

    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.csrf(AbstractHttpConfigurer::disable)
            .headers(headersConfigurer -> headersConfigurer.frameOptions(
                HeadersConfigurer.FrameOptionsConfig::disable)) // For H2 DB
            .formLogin(AbstractHttpConfigurer::disable)
            .httpBasic(AbstractHttpConfigurer::disable)
            .authorizeHttpRequests((requests) -> requests
                .requestMatchers(antMatcher("/edupi_user/oauth2/**")).permitAll()
                .anyRequest().authenticated()
            )
            .sessionManagement(sessions -> sessions
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS))

            .oauth2Login(configure ->
                configure
                    .authorizationEndpoint(config -> {
                        config.baseUri("/edupi_user/oauth2/authorization");
                        config.authorizationRequestRepository(httpCookieOAuth2AuthorizationRequestRepository);
                    })
                    .userInfoEndpoint(config ->
                        config.userService(customOAuth2UserService))
                    .successHandler(oAuth2AuthenticationSuccessHandler) // 인증 성공 시 처리
                    .failureHandler(oAuth2AuthenticationFailureHandler) //  인증 실패 시 처리
            );

        http.addFilterBefore(jwtAuthorizationFilter, UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }
}