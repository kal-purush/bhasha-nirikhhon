package com.eidiko.supermarket_integration_service.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;

@OpenAPIDefinition(
        info = @Info(
                title = "Supermarket Integration Service",
                description = "API for supermarket integration service",
                version = "v1.0",
                contact = @Contact(
                        name = "Adil Raza",
                        email = "mdadilraza46@gmail.com"
                )
        )
)
public class SwaggerConfig {
}