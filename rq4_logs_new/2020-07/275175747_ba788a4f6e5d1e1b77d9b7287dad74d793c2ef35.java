package jm.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springdoc.core.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

    @Bean
    public GroupedOpenApi userOpenApi() {
        String[] path = {"/rest/api/users/**"};
        return getGroupedOpenApi(path, "user");
    }

    @Bean
    public GroupedOpenApi adminOpenApi() {
        String[] path = {"/rest/api/admin/**"};
        return getGroupedOpenApi(path, "admin");
    }

    @Bean
    public OpenAPI customOpenAPI() {
        Contact contact = new Contact()
                .url("https://github.com/NikitaNesterenko/JM-PROJECT");

        License license = new License()
                .name("Apache License Version 2.0")
                .url("https://www.apache.org/licesen.html");

        Info info = new Info()
                .title("JM-Stockx-project")
                .version("1.0")
                .contact(contact)
                .license(license);

        return new OpenAPI()
                .components(
                        new Components().addSecuritySchemes("basicScheme",
                                new SecurityScheme().type(SecurityScheme.Type.HTTP).scheme("basic"))
                )
                .info(info);
    }

    private GroupedOpenApi getGroupedOpenApi(String[] path, String channels) {
        return GroupedOpenApi
                .builder()
                .setGroup(channels)
                .pathsToMatch(path)
                .build();
    }
}