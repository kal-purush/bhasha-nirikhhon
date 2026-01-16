package com.example.taskmanager.controller;

import com.example.taskmanager.dto.categoria.DadosCriarCategoria;
import com.example.taskmanager.dto.categoria.DadosListagemCategoria;
import com.example.taskmanager.service.CategoriaService;
import jakarta.validation.Valid;
import org.apache.coyote.Response;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

@RestController
@RequestMapping("/usuarios/{usuarioId}/categorias")
public class CategoriaController {

    private final CategoriaService categoriaService;

    public CategoriaController(CategoriaService categoriaService) {
        this.categoriaService = categoriaService;
    }

    @PostMapping
    public ResponseEntity<DadosListagemCategoria> criarCategoria(@PathVariable Long usuarioId,
                                                                 @RequestBody @Valid DadosCriarCategoria dados,
                                                                 UriComponentsBuilder uriBuilder) {

        return categoriaService.criarCategoria(dados, usuarioId, uriBuilder);
    }

}