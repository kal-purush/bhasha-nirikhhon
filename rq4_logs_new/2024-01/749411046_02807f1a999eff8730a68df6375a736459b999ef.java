package com.example.s27454bank.controller;

import com.example.s27454bank.model.konto.Konto;
import com.example.s27454bank.service.KontoService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/konto")
@RequiredArgsConstructor
public class KontoController {
    private final KontoService kontoService;

    @PostMapping("/create")
    public ResponseEntity<Konto> createKonto(@RequestBody Konto konto) {
        Konto createdKonto = kontoService.createKonto(konto);

        return ResponseEntity
                .status(HttpStatusCode.valueOf(201))
                .body(createdKonto);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Konto> getKontoByPathVariable(@PathVariable Integer id) {
        Konto konto = kontoService.getKontoById(id);

        return ResponseEntity.ok(konto);
    }

    @GetMapping("/more_than")
    public ResponseEntity<List<Konto>> listKontoWithBalanceHigherThanMinBalance(@RequestParam(name = "minBalance") Double minBalance) {
        List<Konto> kontoList = kontoService.listKontoWithHigherBalanceThan(minBalance);

        return ResponseEntity.ok(kontoList);
    }
}