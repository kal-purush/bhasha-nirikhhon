package com.customs.network.fdapn.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@OpenAPIDefinition
public class SwaggerConfiguration implements WebMvcConfigurer {

    @Value("${swagger.project-description: customs-network}")
    private String projectDescription;

    @Value("${swagger.support-email: info@seabed2.com}")
    private String supportEmail;

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry.addResourceHandler("/swagger-ui.html**")
                .addResourceLocations("classpath:/META-INF/resources/");
        registry.addResourceHandler("/webjars/**")
                .addResourceLocations("classpath:/META-INF/resources/webjars/");
    }

    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI().info(new Info().title("customs-network")
                .version("1.0")
                .description(projectDescription)
                .contact(new io.swagger.v3.oas.models.info.Contact().name("SEABED2CREST").email(supportEmail)));
    }
}