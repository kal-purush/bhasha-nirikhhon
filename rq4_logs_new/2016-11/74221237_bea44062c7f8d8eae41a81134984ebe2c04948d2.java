package modelo;


import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class ProdutoTest {

    public ProdutoTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testGetSetCodigo() {
        System.out.println("getNome");
        Produto produto = new Produto();
        produto.setCodigo(10);
        assertEquals(10, produto.getCodigo());
    }

    @Test
    public void testGetSetNome() {
        System.out.println("getNome");
        Produto produto = new Produto();
        produto.setNome("guaraná");
        assertEquals("guaraná", produto.getNome());
    }

    @Test
    public void testGetSetProduto_Prato_codigo() {
        System.out.println("getProduto_Prato_codigo");
        Produto produto = new Produto();
        produto.setProduto_Prato_codigo(1007);
        assertEquals(1007, produto.getProduto_Prato_codigo());
    }

    @Test
    public void testGetSetProduto_Bebida_codigo() {
        System.out.println("getProduto_Bebida_codigo");
        Produto produto = new Produto();
        produto.setProduto_Bebida_codigo(2007);
        assertEquals(2007, produto.getProduto_Bebida_codigo());
    }

    /*@Test
    public void testGetSetPreco() {
        System.out.println("getPreco");
        Produto produto = new Produto();
        produto.setPreco(10);
        assertEquals(10, produto.getPreco());
    }*/

}