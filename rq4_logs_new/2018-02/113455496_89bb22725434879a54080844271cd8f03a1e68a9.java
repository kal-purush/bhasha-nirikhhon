package tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import comparators.ComparaApostas;
import entidades.Cenario;
import entidades.CenarioDefault;

/**
 * Classe de Testes JUnity da classe ComparaApostas.
 * 
 * Lab05 - Laboratório de Programação II
 * 
 * @author Héricles Emanuel - 117110647
 *
 */
public class ComparaApostasTest {
	ComparaApostas ca;
	Cenario c1, c2;

	/**
	 * Inicializa os cenários c1 e c2.
	 */
	@Before
	public void inicializa() {
		c1 = new CenarioDefault("mamae passou", 1);
		c2 = new CenarioDefault("açuca in mim", 2);
	}

	/**
	 * Testa o método compare, pela ordem da quantidade de apostas, quando c1 tiver
	 * mais apostas que c2.
	 */
	@Test
	public void testCompareMaior() {
		ca = new ComparaApostas();
		c1.cadastraAposta("por isso", 1900, "N VAI ACONTECER");
		assertEquals(-1, ca.compare(c1, c2));
	}

	/**
	 * Testa o método compare, pela ordem da quantidade de apostas, quando c1 tiver
	 * menos apostas que c2.
	 */
	@Test
	public void testCompareMenor() {
		ca = new ComparaApostas();
		c2.cadastraAposta("sou gostosinho assim", 1900, "N VAI ACONTECER");
		assertEquals(1, ca.compare(c1, c2));
	}

	/**
	 * Testa o método compare, pela ordem da quantidade de apostas, quando c1 tiver
	 * a mesma quantidade de apostas que c2.
	 */
	@Test
	public void testCompareIgual() {
		ca = new ComparaApostas();
		c1.cadastraAposta("confia na call", 1900, "N VAI ACONTECER");
		c2.cadastraAposta("xdxd", 1900, "N VAI ACONTECER");
		assertEquals(0, ca.compare(c1, c2));
	}

	/**
	 * Testa o construtor de um comparador por apostas.
	 */
	@Test
	public void testConstrutor() {
		assertTrue(ca == null);
		ca = new ComparaApostas();
		assertFalse(ca == null);
	}

}