package com.training.java.grandmassfood.delivery.api.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(
        info = @Info(
                title = "Grandmas food delivery api",
                version = "1.0",
                description = "This is a API for Grandmas Foods Delivery"
        )
)
public class OpenApiConfig {
}