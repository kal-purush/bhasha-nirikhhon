package com.api.financas.controller;

import com.api.financas.dto.DespesaVariavelRequestDTO;
import com.api.financas.exceptions.GenericaException;
import com.api.financas.service.DespesaVariavelService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;

@RestController
@RequestMapping("/api/despesa-variavel")
public class DespesaVariavelController {

    @Autowired
    private DespesaVariavelService service;

    @PostMapping("/salvar/{id}")
    public ResponseEntity<Object> salvar(@PathVariable String id, @RequestBody @Valid DespesaVariavelRequestDTO despesaVariavelRequestDTO) {
        try {
            return ResponseEntity.ok().body(service.criar(id, despesaVariavelRequestDTO));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }

    @PostMapping("/alterar/{id}")
    public ResponseEntity<Object> alterar(@PathVariable String id, @Valid @RequestBody DespesaVariavelRequestDTO despesaVariavelRequestDTO) {
        try {
            return ResponseEntity.ok().body(service.alterar(id, despesaVariavelRequestDTO));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }

    @GetMapping("/listar/{id}")
    public ResponseEntity<Object> listarTodos(@PathVariable String id) throws GenericaException {
        return ResponseEntity.ok().body(service.listarTodos(id));
    }

    @GetMapping("/listar-por-data/id/{id}/data/{data}")
    public ResponseEntity<Object> listarPorData(@PathVariable String id, @PathVariable LocalDate data)
            throws GenericaException {
        return ResponseEntity.ok().body(service.listarPorData(id, data));
    }

    @DeleteMapping("/deletar/{id}")
    public ResponseEntity<Void> deletar(@PathVariable String id) throws GenericaException {
        service.deletar(id);
        return ResponseEntity.noContent().build();
    }
}