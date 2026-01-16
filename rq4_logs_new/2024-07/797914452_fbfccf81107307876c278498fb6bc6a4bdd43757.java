package com.codeconnect.usuario.dto;

import com.codeconnect.redesocial.dto.RedeSocialResponse;
import com.codeconnect.usuario.enums.UsuarioAmigoStatusEnum;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.UUID;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class UsuarioPerfilResponse {

    @Schema(description = "Id usuário", example = "b2adf87b-b98a-49e3-af3f-57f8e5ac467d")
    private UUID id;

    @Schema(description = "Nome usuário", example = "Joao")
    private String nome;

    @Schema(description = "Profissão do usuário", example = "Engenheiro de Software")
    private String profissao;

    @Schema(description = "País do usuário", example = "Brasil")
    private String pais;

    @Schema(description = "Estado do usuário", example = "São Paulo")
    private String estado;

    @Schema(description = "Retornar usuario logado")
    @JsonProperty("usuario_logado")
    private boolean usuarioLogado;

    @Schema(description = "Retorna status do relacionamento de amizade")
    @JsonProperty("status_relacionamento")
    @Enumerated(EnumType.ORDINAL)
    private UsuarioAmigoStatusEnum statusRelacionamento;

    @Schema(description = "Lista de redes sociais do usuário")
    @JsonProperty("redes_sociais")
    private List<RedeSocialResponse> redesSociais;

}