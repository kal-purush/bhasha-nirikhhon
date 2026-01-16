package com.example.taskmanager.dto.tarefa;

import org.springframework.data.domain.Page;

import java.util.List;

public record PaginadoTarefaDTO(
        List<DadosListagemTarefa> tarefas,
        int paginaAtual,
        int totalItens,
        int totalPaginas
) {

    public static PaginadoTarefaDTO from(Page<DadosListagemTarefa> page) {
        return new PaginadoTarefaDTO(
                page.getContent(),
                page.getNumber(),
                (int) page.getTotalElements(),
                page.getTotalPages()
        );
    }
}