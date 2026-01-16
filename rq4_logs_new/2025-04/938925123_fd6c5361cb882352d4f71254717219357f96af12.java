package com.example.taskmanager.controller;

import com.example.taskmanager.dto.usuario.DadosListagemUsuarioDTO;
import com.example.taskmanager.service.UsuarioService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final UsuarioService usuarioService;

    public AdminController(UsuarioService usuarioService) {
        this.usuarioService = usuarioService;
    }

    @PostMapping("/usuarios/{id}/promover")
    public ResponseEntity<DadosListagemUsuarioDTO> promoverParaAdmin(@PathVariable Long id) {
        return usuarioService.promoverParaAdmin(id);
    }

    @DeleteMapping("/usuarios/{id}")
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<DadosListagemUsuarioDTO> inativarUsuario(@PathVariable Long id) {
        usuarioService.inativarUsuarioComoAdmin(id);
        return ResponseEntity.noContent().build();
    }


}