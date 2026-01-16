package modelo;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.MockitoAnnotations;

public class PedidoTest {

    public PedidoTest() {
    }

    private Pedido pedido;

    @Mock
    private Produto produto;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        pedido = new Pedido();
    }

    @Test
    public void testAddQuantidade() {
        when(produto.getPreco()).thenReturn(50.0);

        pedido.adicionarItem(2, produto);

        assertEquals(pedido.getValor(), 100.0, 0);
    }
}