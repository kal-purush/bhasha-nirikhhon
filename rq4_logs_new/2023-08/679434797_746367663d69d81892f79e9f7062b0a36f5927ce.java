package br.com.dbc.vemser.ecommerce.dto.usuario;

import br.com.dbc.vemser.ecommerce.entity.enums.Cargo;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserAtualizacaoDTO {


    private String login;


    @Schema(description = "Cargo so usuário", type = "string", allowableValues = {"ROLE_ADMIN", "ROLE_USUARIO", "ROLE_VISITANTE"})
    private Cargo cargo;
}