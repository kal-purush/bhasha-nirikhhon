package EXAMEN;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TrenTest {

	Tren tren;
	static final String TIPO = "locomotora";
	static final int REFERENCIA = 34566;
	static final int ASIENTOS_OCUPADOS =300;
	static final int ANYOS = 36;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		tren = new Tren(TIPO, REFERENCIA, ASIENTOS_OCUPADOS, ANYOS);
	}

	@After
	public void tearDown() throws Exception {
		tren = null;
	}

	@Test
	public void testMostrar() {
		fail("Not yet implemented");
	}

	@Test
	public void testToString() {
		fail("Not yet implemented");
	}

	@Test
	public void testTrenLleno() {
		fail("Not yet implemented");
	}

	@Test
	public void testDemasiadoViejo() {
		fail("Not yet implemented");
	}

	@Test
	public void testTrenStringIntIntInt() {
		fail("Not yet implemented");
	}

	@Test
	public void testTren() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetTipo() {
		assertEquals(TIPO, tren.getTipo());
	}

	@Test
	public void testSetTipo() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetReferencia() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetReferencia() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetAsientosOcupados() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetAsientosOcupados() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetA�osActivo() {
		fail("Not yet implemented");
	}

	@Test
	public void testSetA�osActivo() {
		fail("Not yet implemented");
	}

}