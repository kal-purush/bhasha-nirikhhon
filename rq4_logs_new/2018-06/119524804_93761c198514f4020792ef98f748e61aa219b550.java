package modeltest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import model.Agent;
import model.Incidence;
import model.Localizacion;
import model.util.ModelException;

public class IncidenceTest {

	@Test
	public void test() {
		try {
			Agent agente = new Agent("Dani",null,"dani35@gmail.com","dani123","Ciudadano");
			Incidence incidence = new Incidence(agente, "incidencia", "Se ha producido una incidencia", new Localizacion(43,-6).toString(), null);
			assertEquals(incidence.getIncidenceName(),"incidencia");
			assertFalse(incidence.isPeligrosa());
			incidence.setDescription("peligro");
			assertTrue(incidence.isPeligrosa());
		} catch (ModelException e) {
			System.err.println("Se produjo error en el modelo");
		}
		
	}

}