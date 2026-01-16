package br.com.zup.TaDentro.colaboradores;

import br.com.zup.TaDentro.Usuario.Usuario;
import br.com.zup.TaDentro.colaborador.Colaborador;
import br.com.zup.TaDentro.colaborador.ColaboradorController;
import br.com.zup.TaDentro.colaborador.ColaboradorService;
import br.com.zup.TaDentro.enums.Cargo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest (ColaboradorController.class)
public class ColaboradorControllerTest {

    @MockBean
    private ColaboradorService colaboradorService;

    @Autowired
    private MockMvc mockMvc;



    /*@BeforeEach
    public void setUp () {
        colaborador = new Colaborador();
        usuario = new Usuario();

        colaborador.setNome("Lucas");
        colaborador.setEmail("lucas@123.com");
        colaborador.setCargo(Cargo.DEV_JR);
        colaborador.setLoginUsuario(usuario);*/

    }

