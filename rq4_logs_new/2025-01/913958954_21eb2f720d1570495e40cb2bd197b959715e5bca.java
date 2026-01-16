package com.api.cep;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.api.cep.entity.Usuarios;
import com.api.cep.repository.UsuariosRepository;
import com.api.cep.service.UsuariosService;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class UsuariosServiceTest {

    @InjectMocks
    private UsuariosService usuariosService;

    @Mock
    private UsuariosRepository usuariosRepository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testSaveOrUpdate_NewUser() {
        Usuarios usuario = new Usuarios();
        usuario.setNome("Novo Usuário");
        usuario.setCpf("12345678901");

        when(usuariosRepository.save(any(Usuarios.class))).thenReturn(usuario);

        Usuarios result = usuariosService.saveOrUpdate(usuario);

        assertNotNull(result);
        assertEquals("Novo Usuário", result.getNome());
        verify(usuariosRepository, times(1)).save(any(Usuarios.class));
    }

    @Test
    void testSaveOrUpdate_UpdateExistingUser() {
        Usuarios existingUser = new Usuarios();
        existingUser.setId(1L);
        existingUser.setNome("Usuário Atualizado");
        existingUser.setCpf("12345678901");

        when(usuariosRepository.findById(1L)).thenReturn(Optional.of(existingUser));
        when(usuariosRepository.save(any(Usuarios.class))).thenReturn(existingUser);

        Usuarios result = usuariosService.saveOrUpdate(existingUser);

        assertNotNull(result);
        assertEquals("Usuário Atualizado", result.getNome());
        verify(usuariosRepository, times(1)).findById(1L);
        verify(usuariosRepository, times(1)).save(any(Usuarios.class));
    }

    @Test
    void testSaveOrUpdate_UserNotFound() {
        Usuarios nonExistingUser = new Usuarios();
        nonExistingUser.setId(99L);

        when(usuariosRepository.findById(99L)).thenReturn(Optional.empty());

        assertThrows(IllegalArgumentException.class, () -> usuariosService.saveOrUpdate(nonExistingUser));
        verify(usuariosRepository, times(1)).findById(99L);
        verify(usuariosRepository, times(0)).save(any(Usuarios.class));
    }
}
