package com.example.taskmanager.dto.categoria;

import org.springframework.data.domain.Page;

import java.util.List;

public record PaginaCategoriaDTO (
        List<DadosListagemCategoria> categorias,
        int paginaAtual,
        int totalItens,
        int totalPaginas
){

    public static PaginaCategoriaDTO from(Page<DadosListagemCategoria> page) {
        return new PaginaCategoriaDTO(
                page.getContent(),
                page.getNumber(),
                (int) page.getTotalElements(),
                page.getTotalPages()
        );
    }
}