package br.com.fiap.fiapfood.domain.entity;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class UsuarioDomain {

    private Long id;
    private String nome;
    private String email;
    private String cpf;

    public UsuarioDomain(String nome, String email, String cpf) {

        if (nome.isBlank() || nome.isEmpty()) {
            throw new RuntimeException("Nome inválido");
        }
        if (email.isBlank() || email.isEmpty()) {
            throw new RuntimeException("Email inválido");
        }
        if (cpf.isBlank() || cpf.isEmpty()) {
            throw new RuntimeException("CPF inválido");
        }

        this.nome = nome;
        this.email = email;
        this.cpf = cpf;
    }

}