package org.example.todoapp.configs;

import java.util.*;

import io.swagger.v3.oas.models.*;
import io.swagger.v3.oas.models.info.*;
import io.swagger.v3.oas.models.servers.*;
import org.springdoc.core.models.*;
import org.springframework.context.annotation.*;

@Configuration
public class SwaggerConfig {
    @Bean
    public OpenAPI api() {
        return new OpenAPI()
            .servers(
                List.of(
                    new Server().url("http://localhost:8686")
                )
            )
            .info(
                new Info()
                    .title("Todo app API")
                    .description("API для To do приложения")
                    .version("1.0")
            );
    }
}