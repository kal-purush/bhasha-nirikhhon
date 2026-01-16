package modeltest;

import static org.junit.Assert.*;

import org.junit.Test;

import model.Agent;
import model.Localizacion;
import model.Operario;
import model.util.ModelException;

public class OperarioTest {

	@Test
	public void testEquals() throws ModelException {		
	
		//
		Operario oper1 = new Operario("oper1@gmail.com","123456");
		Operario oper2 = new Operario("oper2@gmail.com","123456");
		Operario oper3 = new Operario("oper3@gmail.com","123456");

		assertNotNull(oper1);
		assertNotNull(oper2);
		assertNotNull(oper3);
	}


	@Test(expected = ModelException.class)
	public void testConstructor() throws ModelException {
		Operario oper1 = new Operario("oper1@gmail.com","123456");
		
		assertNotNull(oper1);
		assertEquals("oper1@gmail.com", oper1.getEmail());
		assertEquals("123456", oper1.getPassword());
		
		//ahora creamos un operario mal
		oper1 = new Operario("oper1@gmail.com", null);
	}
	
	@Test
	public void testToString() throws ModelException {
		Operario oper1 = new Operario("oper1@gmail.com","123456");
		
		assertEquals(oper1.toString(), "Operador [id=null, email=oper1@gmail.com, password=123456, role=null, partes=[]]");
	}

}