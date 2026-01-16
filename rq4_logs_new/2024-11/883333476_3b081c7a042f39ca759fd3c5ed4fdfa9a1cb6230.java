package org.rentifytools.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**") // Разрешить CORS для всех маршрутов
                .allowedOrigins("http://localhost:5173") // Разрешить запросы с фронтенда
                .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH") // Методы HTTP
                .allowedHeaders("*") // Все заголовки
                .allowCredentials(true); // Если используются cookies или авторизация
    }
}