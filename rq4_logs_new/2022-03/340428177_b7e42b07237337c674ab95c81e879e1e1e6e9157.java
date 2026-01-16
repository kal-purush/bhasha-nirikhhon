package com.lafinance.dashboard.controller;

import com.lafinance.dashboard.dto.RelatorioCompraDTO;
import com.lafinance.dashboard.service.RelatorioCompraService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@CrossOrigin
@RestController
@RequestMapping("/api/relatorio_compra")
public class RelatorioCompraResource {

    @Autowired
    private RelatorioCompraService relatorioCompraService;

    @CrossOrigin
    @GetMapping("/consulta/mes/{mes}/{ano}")
    public ResponseEntity<List<RelatorioCompraDTO>> consultarDadosCompraPorMes(
            @PathVariable(name = "mes") Integer mes,
            @PathVariable(name = "ano") Integer ano) {
        try {
            return ResponseEntity.ok().body(relatorioCompraService.consultarDadosCompraPorMes(mes, ano));
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @CrossOrigin
    @GetMapping("/consulta/ano/{ano}")
    public ResponseEntity<List<RelatorioCompraDTO>> consultarDadosCompraAno(
            @PathVariable(name = "ano") Integer ano) {
        try {
            return ResponseEntity.ok().body(relatorioCompraService.consultarDadosCompraPorAno(ano));
        } catch (Exception e) {
            return ResponseEntity.badRequest().build();
        }
    }

}