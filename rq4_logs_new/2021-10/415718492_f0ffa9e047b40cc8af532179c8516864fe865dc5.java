package dad.rubenpablo.cambiodivisa;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DivisaTest {
	
	@Test
	public void cambioEuroAYen() {
		assertEquals((Double)667.95, Divisa.fromTo(Divisa.Euro, Divisa.Yen, 5.0));
	}
	
	@Test
	public void cambioYenAEuro() {
		assertEquals((Double)5.0, Divisa.fromTo(Divisa.Yen, Divisa.Euro, 667.95));
	}

}