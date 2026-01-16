package com.jaimayal.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

@Configuration
public class CorsConfig implements WebMvcConfigurer {
    
    @Value("#{'${cors.allowed-origins}'.split(', ')}")
    private List<String> allowedOrigins;
    @Value("#{'${cors.allowed-methods}'.split(', ')}")
    private List<String> allowedMethods;
    @Value("#{'${cors.allowed-headers}'.split(', ')}")
    private List<String> allowedHeaders;
    @Value("#{'${cors.exposed-headers}'.split(', ')}")
    private List<String> exposedHeaders;
    
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOrigins("*")
                .allowedMethods("GET", "POST", "PUT", "DELETE");
    }
    
    @Bean
    public CorsConfigurationSource getCorsConfigurationSource() {
        CorsConfiguration corsConfiguration = new CorsConfiguration();
        corsConfiguration.setAllowedOrigins(this.allowedOrigins);
        corsConfiguration.setAllowedMethods(this.allowedMethods);
        corsConfiguration.setAllowedHeaders(this.allowedHeaders);
        corsConfiguration.setExposedHeaders(this.exposedHeaders);
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/api/**", corsConfiguration);
        return source;
    }
}