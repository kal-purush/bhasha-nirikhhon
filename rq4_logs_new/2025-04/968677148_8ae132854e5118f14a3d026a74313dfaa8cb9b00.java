package com.unifucamp.gamearena.controller;

import com.unifucamp.gamearena.controller.dto.InscricaoDTO;
import com.unifucamp.gamearena.domain.Inscricao;
import com.unifucamp.gamearena.mapper.InscricaoMapper;
import com.unifucamp.gamearena.service.InscricaoService;
import jakarta.validation.Valid;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/inscricoes")
public class InscricaoController {

    private final InscricaoService inscricaoService;

    public InscricaoController(InscricaoService inscricaoService) {
        this.inscricaoService = inscricaoService;
    }

    @PostMapping
    public ResponseEntity<InscricaoDTO> criarInscricao(@RequestBody @Valid InscricaoDTO inscricaoDTO) {
        Inscricao inscricaoSalva = inscricaoService.salvar(InscricaoMapper.dtoToDomain(inscricaoDTO));
        return ResponseEntity.ok(InscricaoMapper.domainToDTO(inscricaoSalva));
    }

    @PutMapping("/{id}")
    public ResponseEntity<InscricaoDTO> atualizarInscricao(@PathVariable Integer id, @RequestBody InscricaoDTO inscricaoDTO) {
        return inscricaoService.atualizar(id, InscricaoMapper.dtoToDomain(inscricaoDTO))
                .map(inscricao -> ResponseEntity.ok(InscricaoMapper.domainToDTO(inscricao)))
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/{id}")
    public ResponseEntity<InscricaoDTO> buscarPorId(@PathVariable Integer id) {
        return inscricaoService.buscarPorId(id)
                .map(inscricao -> ResponseEntity.ok(InscricaoMapper.domainToDTO(inscricao)))
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<InscricaoDTO>> listarTodas(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        Pageable pageable = PageRequest.of(page, size);
        List<Inscricao> inscricoes = inscricaoService.listarTodas(pageable);

        List<InscricaoDTO> inscricoesDTO = inscricoes.stream()
                .map(InscricaoMapper::domainToDTO)
                .collect(Collectors.toList());
        return ResponseEntity.ok(inscricoesDTO);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> desativarInscricao(@PathVariable Integer id) {
        boolean desativado = inscricaoService.desativar(id);
        if (desativado) {
            return ResponseEntity.noContent().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
}