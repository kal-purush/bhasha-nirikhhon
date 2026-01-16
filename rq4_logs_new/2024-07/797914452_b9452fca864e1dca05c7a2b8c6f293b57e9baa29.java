package com.codeconnect.redesocial.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RedeSocialRequest {

    @Schema(description = "Nome da rede social", example = "Github")
    private String nome;

    @Schema(description = "Link da rede social", example = "https://www.github.com/in/joao")
    private String link;

}