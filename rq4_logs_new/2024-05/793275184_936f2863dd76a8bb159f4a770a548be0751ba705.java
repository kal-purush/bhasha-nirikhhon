package test;

import datos.Contacto;
import negocio.ContactoABM;

public class TestActualizarContacto {
	public static void main(String[] args) throws Exception {
	ContactoABM abmContacto = new ContactoABM();
	
	
	Contacto contacto = abmContacto.traer(4L);
	

	

	try{
		abmContacto.modificar(contacto.modificar(contacto, "eze@unla.edu.ar", "1132113211", "41214145"));
		System.out.println("El cliente se modificó con éxito");
	}catch (Exception e) {
		throw e;
	}
	
	
	try{
		abmContacto.modificar(contacto.modificar(contacto, "ajaramillo@unla.edu.ar", "1132113211", "41214145"));
		System.out.println("El cliente se modificó con éxito");
	}catch (Exception e) {
		throw e;
	}
}}