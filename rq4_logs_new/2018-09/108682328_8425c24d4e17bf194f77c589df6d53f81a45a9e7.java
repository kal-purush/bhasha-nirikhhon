package com.systelab.seed.resource;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;

@OpenAPIDefinition(
        info = @Info(
                title = "Seed application programming interface (API).",
                version = "1.0.2",
                description = "Restful API to manage the Seed Application to be used as an example.",
                license = @License(name = "MIT", url = "https://opensource.org/licenses/MIT"),
                contact = @Contact(url = "http://www.werfen.com", name = "Werfen Clinical Software", email = "support@werfen.com")
        ),
        servers = {
                @Server(
                        description = "Server",
                        url = "/seed")
        },
        tags= {@Tag(name = "Patient", description = "Patient Management"),
        @Tag(name = "User", description = "User Management"), @Tag(name = "Health", description = "Health checking")})
@SecurityScheme(
        name = "Authorization",
        type = SecuritySchemeType.HTTP,
        description = "JWT Token Authentication",
        scheme = "bearer"
)
public class SeedOpenApiDefinition {
}