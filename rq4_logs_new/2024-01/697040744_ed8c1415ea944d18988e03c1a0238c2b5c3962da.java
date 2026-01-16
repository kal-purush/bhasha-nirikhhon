package br.com.fiap.soat.grupo48.application.commons.model;

import br.com.fiap.soat.grupo48.application.commons.exception.NomeException;
import lombok.SneakyThrows;

public record Nome(String nome) {
    @SneakyThrows
    public Nome {
        if (nome.isEmpty()) {
            throw new NomeException("O nome do produto não pode ser vazio");
        } else if (nome.length() < 3) {
            throw  new NomeException("O nome do produto tem que ter o valor minimo de 3 caracteres");
        }
    }
}