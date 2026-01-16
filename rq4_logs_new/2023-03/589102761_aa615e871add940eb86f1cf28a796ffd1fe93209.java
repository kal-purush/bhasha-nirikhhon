package com.pokemon.pokedex.documentation;

import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;

@SecurityScheme(name = "JWT",
        type = SecuritySchemeType.HTTP,
        scheme = "bearer")
@SecurityRequirement(name = "JWT", scopes = "write: read")
public class SecurityDocumentation {
}