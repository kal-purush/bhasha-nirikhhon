package com.api.financas.controller;

import com.api.financas.dto.DespesaFixaRequestDTO;
import com.api.financas.service.DespesaFixaService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/despesa-fixa")
public class DespesaFixaController {

    @Autowired
    private DespesaFixaService service;

    @PostMapping("/salvar/{id}")
    public ResponseEntity<Object> salvar(@PathVariable String id, @RequestBody @Valid DespesaFixaRequestDTO despesaFixaRequestDTO){
        try {
            return ResponseEntity.ok().body(service.criar(id, despesaFixaRequestDTO));
        }catch (Exception e){
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage());
        }
    }

    @GetMapping("/listar/{id}")
    public ResponseEntity<Object> listarTodos(@PathVariable String id) {
        return ResponseEntity.ok().body(service.listarTodos(id));
    }
}