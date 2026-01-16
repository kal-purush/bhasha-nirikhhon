import java.sql.ResultSet;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class PanelRegistroTest extends TestCase{

	private PanelRegistro pRegis ;
	private String nombreUsuario ;
	
	private Cliente cliente ;
	
	private Cliente sesion;
	
	
	
	@Before
	 public void setUp() throws Exception {
		
		pRegis = new PanelRegistro();
		
		nombreUsuario = "";
			
			
	 } 
	
	@After
	 public void tearDown() {
	 
			
	 }
	
	@Test
	public void testNotNull() {
		
		
		assertNotNull(pRegis);
		
	}
	
	@Test
	public void testSesion() {
		
		pRegis.pasarSesion(sesion);
		
	}
	
	
	
	
	/*
	@Test 
	public void testComprobarUsuario( String nombreUsuario) {
		
		BaseDeDatos.comprobarUsuario (nombreUsuario);
		
		
		if(BaseDeDatos.insertarCliente(cliente.getNombreusuario());
		

		
	}*/
	
	@Test
	public void testUsuario() {
		
		assertEquals(cliente.getNombreusuario (), "ane");
		
	}
	
	
	
}