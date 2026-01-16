package com.filegenie.backend.Configs;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

    @Bean
    public SecurityFilterChain configure(HttpSecurity http) throws Exception {
        http
            // CSRF does not work well with Swagger UI
            // TODO: find a way to make it works without disabling it
            .csrf().disable()
            // Turn Spring auth login panel off
            // TODO: activate on production
            .authorizeHttpRequests(request -> request.anyRequest().permitAll());

        return http.build();
    }
}