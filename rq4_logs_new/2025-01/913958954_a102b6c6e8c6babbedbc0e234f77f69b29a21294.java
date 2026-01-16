package com.api.cep.service;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.stereotype.Service;

import com.api.cep.entity.Usuarios;
import com.api.cep.repository.UsuariosRepository;

import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class UsuariosService {

    private UsuariosRepository usuariosRepository;

    @Transactional
    public Usuarios saveOrUpdate(Usuarios usuario) {

        if (usuario.getId() != null) {
            usuario.setDataAtualizacao(LocalDateTime.now());
        } else {
            usuario.setDataCriacao(LocalDateTime.now());
        }

        usuariosRepository.save(usuario);

        return usuario;
    }

    public List<Usuarios> findAll() {
        return usuariosRepository.findAll();
    }

    @Transactional
    public void delete(Long id) {
        if (!usuariosRepository.existsById(id)) {
            throw new IllegalArgumentException("Usuário não encontrado: " + id);
        }
        usuariosRepository.deleteById(id);
    }

}