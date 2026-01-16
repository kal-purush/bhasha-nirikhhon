package com.api.cep;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

import com.api.cep.controller.UsuariosController;
import com.api.cep.entity.Usuarios;
import com.api.cep.service.UsuariosService;

@WebMvcTest(UsuariosController.class)
class UsuariosControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @SuppressWarnings("removal")
    @MockBean
    private UsuariosService usuariosService;

    @Test
    void testSaveUsuario() throws Exception {
        // Mock do usuário de entrada
        Usuarios usuario = new Usuarios();
        usuario.setNome("João Silva");
        usuario.setCpf("12345678900");
        usuario.setCep("12345-678");

        // Mock do serviço
        when(usuariosService.saveOrUpdate(any(Usuarios.class))).thenReturn(usuario);

        // Executa o teste
        mockMvc.perform(post("/api/usuarios/save")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {
                            "nome": "João Silva",
                            "cpf": "12345678900",
                            "cep": "12345-678"
                        }
                        """))
                .andExpect(status().isNoContent());

        // Verifica se o método foi chamado
        verify(usuariosService, times(1)).saveOrUpdate(any(Usuarios.class));
    }

    @Test
    void testSaveInvalidUsuario() throws Exception {
        // Simula uma exceção lançada no serviço
        when(usuariosService.saveOrUpdate(any(Usuarios.class))).thenThrow(new IllegalArgumentException("CPF é obrigatório"));

        mockMvc.perform(post("/api/usuarios/save")
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {
                            "nome": "João Silva",
                            "cep": "12345-678"
                        }
                        """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value("CPF é obrigatório"));
    }
}