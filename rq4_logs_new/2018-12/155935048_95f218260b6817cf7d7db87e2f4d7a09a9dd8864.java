package br.com.casadocodigo.loja.controllers.test;

import static org.mockito.Mockito.mock;

import br.com.casadocodigo.loja.DAO.ProdutoDAO;
import br.com.casadocodigo.loja.controllers.ProdutosController;
import br.com.casadocodigo.loja.infra.FileSaver;
import br.com.casadocodigo.loja.models.Produto;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.bind.WebDataBinder;

@RunWith(MockitoJUnitRunner.class)
public class ProdutosControllerTest {

    private ProdutosController subject;

    @Mock
    private ProdutoDAO produtoDAO;

    @Mock
    private FileSaver fileSaver;

    @Mock
    private WebDataBinder webDataBinder;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void before() {
        subject = new ProdutosController(produtoDAO, fileSaver);
    }

    @Test
    public void initBinder() {
        subject.initBinder(webDataBinder);
    }

    @Test
    public void form() {
        Produto produto = mock(Produto.class);
        subject.form(produto);
    }
}