package com.codeconnect.post.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.UUID;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PostRecenteDetalheResponse {

    @Schema(description = "Id usuário", example = "b2adf87b-b98a-49e3-af3f-57f8e5ac467d")
    private UUID id;

    @Schema(description = "Data de criação postagem usuário", example = "2024-06-13T18:50:09.719+00:00")
    @JsonProperty("data_criacao")
    private Timestamp dataCriacao;

    @Schema(description = "Descrição da postagem usuário", example = "Bom dia rede, hoje quero compartilhar meu novo projeto.")
    private String descricao;

    @Schema(description = "Detalhes do usuário que fez a postagem")
    private PostRecenteDetalheUsuarioResponse usuario;

}