package br.com.serratec.ecommerce.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import br.com.serratec.ecommerce.model.Log;
import br.com.serratec.ecommerce.service.LogService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api/logs")
@Tag(name = "/api/logs")
public class LogController {

    @Autowired
    private LogService logService;

    @GetMapping
    @PreAuthorize("hasAuthority('admin')")
    @Operation(summary = "método para listar todos os logs cadastrados")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Logs encontrados com sucesso!"),
            @ApiResponse(responseCode = "404", description = "Logs não encontrados"),
            @ApiResponse(responseCode = "500", description = "Erro ao listar os logs"),
            @ApiResponse(responseCode = "504", description = "Tempo da consulta esgotado"),

    })
    public ResponseEntity<List<Log>> obterTodos() {
        return ResponseEntity.ok(logService.obterTodos());
    }

    @GetMapping("/{id}")
    @PreAuthorize("hasAuthority('vendedor')")
    @Operation(summary = "método para buscar log pelo ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Log encontrado com sucesso!"),
            @ApiResponse(responseCode = "400", description = "ID não encontrado"),
            @ApiResponse(responseCode = "404", description = "Log não encontrado"),
            @ApiResponse(responseCode = "500", description = "Erro ao listar o log"),
            @ApiResponse(responseCode = "504", description = "Tempo da consulta esgotado"),

    })
    public ResponseEntity<Log> obterPorId(@PathVariable Long id) {
        return ResponseEntity.ok(logService.obterPorId(id));
    }

    @PostMapping
    @Operation(summary = "método para adicionar log")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Log adicionado com sucesso!"),
            @ApiResponse(responseCode = "404", description = "Não foi possível adicionar o log"),
            @ApiResponse(responseCode = "500", description = "Erro ao adicionar o log"),
            @ApiResponse(responseCode = "504", description = "Tempo da operação esgotado"),

    })
    public ResponseEntity<Log> adicionar(@RequestBody Log log) {
        Log titularAdicionado = logService.adicionar(log);

        return ResponseEntity
                .status(201)
                .body(titularAdicionado);
    }

    @PutMapping("/{id}")
    @Operation(summary = "método para atualizar log")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Log atualizado com sucesso!"),
            @ApiResponse(responseCode = "400", description = "ID não encontrado"),
            @ApiResponse(responseCode = "404", description = "Não foi possível atualizar o log"),
            @ApiResponse(responseCode = "500", description = "Erro ao atualizar o log"),
            @ApiResponse(responseCode = "504", description = "Tempo da operação esgotado"),

    })
    public ResponseEntity<Log> atualizar(@PathVariable Long id, @RequestBody Log log) {
        Log titularAtualizado = logService.atualizar(id, log);

        return ResponseEntity
                .status(200)
                .body(titularAtualizado);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "método para deletar log")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Log deletado com sucesso!"),
            @ApiResponse(responseCode = "400", description = "ID não encontrado"),
            @ApiResponse(responseCode = "404", description = "Não foi possível deletar o log"),
            @ApiResponse(responseCode = "500", description = "Erro ao deletar o log"),
            @ApiResponse(responseCode = "504", description = "Tempo da operação esgotado"),

    })
    public ResponseEntity<?> deletar(@PathVariable Long id) {
        logService.deletar(id);

        return ResponseEntity
                .status(204)
                .build();
    }

}