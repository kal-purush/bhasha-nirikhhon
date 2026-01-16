package com.api.financas.dto.user;

import java.util.UUID;

public record UsuarioLoginDTO(String id, String nome, String email) {
    public UsuarioLoginDTO(String id, String nome, String email) {
        this.id = id;
        this.nome = nome;
        this.email = email;
    }
}