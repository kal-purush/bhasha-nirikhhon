package com.roomfit.be.global.config;


import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {
    @Bean
    public OpenAPI openAPI() {
        Info info = new Info()
                .version("0.0.1")
                .title("Room-fit Project swagger")
                .description("룸핏 프로젝트에 대한 Swagger");

        return new OpenAPI()
                .info(info);
    }
}