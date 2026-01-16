package com.example.taskmanager.controller;

import com.example.taskmanager.dto.tarefa.DadosCriarTarefa;
import com.example.taskmanager.dto.tarefa.DadosListagemTarefa;
import com.example.taskmanager.service.TarefaService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/tarefas")
public class TarefaController {

    public TarefaController(TarefaService tarefaService) {
        this.tarefaService = tarefaService;
    }

    private final TarefaService tarefaService;

    @PostMapping
    public ResponseEntity<DadosListagemTarefa> cadastrar(
            @RequestBody @Valid DadosCriarTarefa dados,
            @RequestParam Long usuarioId,
            @RequestParam(required = false) Long categoriaId,
            UriComponentsBuilder uriBuilder) {
        return tarefaService.criarTarefa(dados, usuarioId, categoriaId, uriBuilder);
    }



}