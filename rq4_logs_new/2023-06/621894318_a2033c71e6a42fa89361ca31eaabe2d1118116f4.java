package com.gxdxx.instagram.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI openAPI() {
        Info info = new Info()
                .version("v1.0.0")
                .title("SNS 서비스")
                .description("SNS 서비스의 API 명세서입니다.");

        return new OpenAPI()
                .info(info)
        ;
    }

}