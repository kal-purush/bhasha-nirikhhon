package com.br.agilize.dash.controller.dashboardController;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import com.br.agilize.dash.model.dto.dashboardDto.PrCountDto;
import com.br.agilize.dash.service.dashboardService.VcsPullRequestService;

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "VcsPullRequestController", description = "Api para gerenciar os VcsPullRequests")
@Controller
@RequestMapping("/prcount")
public class VcsPullRequestController {
     
    @Autowired 
    VcsPullRequestService service;

    @GetMapping("/colaborador/{colaboradorId}")
    public ResponseEntity<PrCountDto> getPrCountByColaboradorId(@PathVariable Long colaboradorId) {
        PrCountDto prCountDto = service.getPrCountByColaboradorId(colaboradorId);
        return ResponseEntity.ok(prCountDto);
    }

}