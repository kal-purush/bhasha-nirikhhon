package uz.pdp.simline.security;

import jakarta.servlet.FilterChain;
import lombok.SneakyThrows;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
@EnableWebSecurity
public class SecurityConfiguration {
    @Bean
    @SneakyThrows
    public SecurityFilterChain securityFilterChain(HttpSecurity  http){
        http.csrf(AbstractHttpConfigurer::disable);
        http.authorizeHttpRequests(registry->
                registry.anyRequest().permitAll()
                );
        return http.build();
    }
}