package ar.com.lokywolf2295.proyectocarritodecompras.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

//Url para ver los Endpoints
//http://localhost:8080/swagger-ui/index.html#/

@Configuration /*la clase con esta configuracipon tiene la capacidad de crear bines, los cuales forman parte de la aplication context de spring/springboot*/
public class OpenApiConfig {
    @Bean
    public OpenAPI OngAPI() {
        final String securitySchemeName = "bearerAuth";
        return new OpenAPI()
                .info(new Info()
                        .title("Proyecto Carrito de Compras")
                        .description("The API REST of Shopping Cart.")
                        .version("v1"))
                .externalDocs(new ExternalDocumentation()
                        .description("")
                        .url(""))
                .addSecurityItem(new SecurityRequirement()
                        .addList(securitySchemeName))
                .components(new Components()
                        .addSecuritySchemes(securitySchemeName, new SecurityScheme()
                                .name(securitySchemeName)
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")));
    }
}