package tests;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;

import clasesNoVisuales.Mario;
import ventanas.VJuego;
import ventanas.VentanaMenuPrincipal;

public class test1 {
	
	@Test
	public void test_caida_salto() {
		Mario mario = new Mario();
		mario.setSalto(false);
		mario.setCaida(false);
		assertEquals(false, mario.isSalto());
		assertEquals(false, mario.isCaida());
	}
	
//	@Test //inacabado
//	public void test_gravedad(){
//		VentanaMenuPrincipal vmp = new VentanaMenuPrincipal();
//		VJuego vJuego = new VJuego(vmp);
//		Mario mario = new Mario();
//		double a = mario.getPosY();
//	}
}