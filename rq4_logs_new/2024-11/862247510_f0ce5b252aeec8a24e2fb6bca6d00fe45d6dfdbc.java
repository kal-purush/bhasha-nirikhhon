package com.example.FenceLink.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**") // Allows CORS for all endpoints
            .allowedOrigins("http://localhost:3000") // Allow only this origin
            .allowedMethods("GET", "POST", "PUT", "DELETE") // Specify allowed methods
            .allowedHeaders("*") // Allow any headers
            .allowCredentials(true); // Allow cookies if needed
    }
}