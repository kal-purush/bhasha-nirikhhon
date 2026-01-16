package br.com.zup.TaDentro.Usuario;

import br.com.zup.TaDentro.Usuario.exceptionUsuario.MensagemErroUsuario;
import br.com.zup.TaDentro.colaborador.Colaborador;
import br.com.zup.TaDentro.colaborador.exceptionColaborador.MensagemErroColaborador;
import br.com.zup.TaDentro.enums.Cargo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.time.LocalDate;

@WithMockUser(username = "xablau", password = "123")
@AutoConfigureMockMvc
@SpringBootTest
public class UsuarioControllerTest {

    @MockBean
    private UsuarioService usuarioService;
    @Autowired
    private MockMvc mockMvc;
    private Usuario usuario;
    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    public void setUp() {

        usuario = new Usuario();
        usuario.setEmail("lucas@123");

        usuario = new Usuario();
        usuario.setNome("Lucas");
        usuario.setEmail("lmatheus@123");
        usuario.setSenha("123");
        objectMapper = new ObjectMapper();

    }

    @Test
    public void testarCadastradoDeUsuarioComSucesso() throws Exception {

        Mockito.when(usuarioService.salvarUsuario(Mockito.any()))
                .thenReturn(usuario);

        String json = objectMapper.writeValueAsString(usuario);

        ResultActions resultadoDaRequisicao = mockMvc
                .perform(MockMvcRequestBuilders.post("/usuarios")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(json))
                .andExpect(MockMvcResultMatchers.status().isCreated());

    }


    @Test
    public void deletarColaboradorCaminhoPositivo() throws Exception {

        Mockito.when(usuarioService.encontrarUsuario(Mockito.anyInt()))
                .thenReturn(usuario);

        usuarioService.encontrarUsuario(1);

        ResultActions resultado = mockMvc.perform(MockMvcRequestBuilders.delete("/usuarios/1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    @Test
    public void testarDeletarColaboradorCaminhoNegativo() throws Exception {

        Mockito.doThrow(MensagemErroUsuario.class).when(usuarioService).deletarUsuario(Mockito.anyInt());

        ResultActions resultado = mockMvc.perform(MockMvcRequestBuilders.delete("/usuarios/1")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isBadRequest());
    }

}