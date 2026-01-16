package com.example.taskmanager.dto.usuario;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record AlterarSenhaDTO(

        @NotBlank(message = "A senha atual é obrigatória")
        String senhaAtual,

        @NotBlank(message = "A nova senha é obrigatória")
        @Size(min = 6, message = "A nova senha deve ter pelo menos 6 caracteres")
        String novaSenha,

        @NotBlank(message = "A confirmação da nova senha é obrigatória")
        String confirmaSenha
) {}
