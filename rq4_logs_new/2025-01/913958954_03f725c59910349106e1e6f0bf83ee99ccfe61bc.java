package com.api.cep.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.api.cep.entity.EnderecoViaCep;
import com.api.cep.service.EnderecoViaCepService;

import io.swagger.v3.oas.annotations.tags.Tag;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/cep")
@Tag(name = "Busca Cep", description = "Busca cep na base de dados ViaCEP.")
@CrossOrigin
public class BuscaCepController {

    @Autowired
    private EnderecoViaCepService enderecoViaCepService;

    @GetMapping("find-by-cep")
    public ResponseEntity<Mono<EnderecoViaCep>> findByCep(String cep) {
        return ResponseEntity.ok(enderecoViaCepService.findByCep(cep));
    }

}