package br.com.zup.TaDentro.colaboradores;

import br.com.zup.TaDentro.colaborador.Colaborador;
import br.com.zup.TaDentro.colaborador.ColaboradorRepository;
import br.com.zup.TaDentro.colaborador.ColaboradorService;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
public class ColaboradorServiceTest {

    @Autowired
    private ColaboradorService colaboradorService;

    @MockBean
    private ColaboradorRepository colaboradorRepository;

    private Colaborador colaborador;

    @BeforeEach
    public void setUp () {
        colaborador = new Colaborador();
    }



}