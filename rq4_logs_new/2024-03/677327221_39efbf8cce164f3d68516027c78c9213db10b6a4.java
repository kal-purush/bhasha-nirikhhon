package com.br.agilize.dash.controller.dashboardController;


import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import com.br.agilize.dash.controller.ControllerCrudBase;
import com.br.agilize.dash.model.dto.dashboardDto.IndiceDeSobrevivenciaDevDto;
import com.br.agilize.dash.service.dashboardService.IndiceDeSobrevivenciaDevService;

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "IndiceDeSobrevivenciaDevController", description = "Api para gerenciar os IndiceDeSobrevivenciaDev")
@Controller
@RequestMapping("/indiceDeSobrevivencia")
public class IndiceDeSobrevivenciaDevController extends ControllerCrudBase<IndiceDeSobrevivenciaDevDto> {
    
    @Autowired IndiceDeSobrevivenciaDevService service;

    public IndiceDeSobrevivenciaDevController(@Autowired IndiceDeSobrevivenciaDevService service) {
        super(service);
    }

    @GetMapping("/colaborador/{colaboradorId}")
    public ResponseEntity<Double> obterValorIndicePorIdColaborador(@PathVariable Long colaboradorId) {
        Double valorIndice = service.obterValorIndicePorIdColaborador(colaboradorId);
        return ResponseEntity.ok(valorIndice);
    }
}