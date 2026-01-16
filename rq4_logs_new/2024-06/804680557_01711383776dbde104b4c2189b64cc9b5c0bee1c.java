package com.ticket.ticketProject.model.dto.usuario;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;

import java.time.LocalDate;

@Builder(toBuilder = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CadastrarUsuarioDTO(
        @NotBlank
        String cpf,
        @NotBlank
        String nome,
        @NotNull
        LocalDate nascimento,
        @NotBlank
        @Email
        String email,
        @NotBlank
        String senha) {
}