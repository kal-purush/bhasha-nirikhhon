package Agenda;
import java.util.ArrayList;
import Contacto.Contacto;

public class Agenda {
	
	private ArrayList<Contacto> contactos;
	
	public Agenda() {
        this.contactos = new ArrayList<>(); // Initialize the list here
	}

	public ArrayList<Contacto> getContactos(){
		return contactos;
	}
	
	public void setContactos(ArrayList<Contacto> contactos) {
		this.contactos = contactos;
	}
	
	public boolean anadirContacto(String nombre, String correo, int numero ) {
		return contactos.add(new Contacto(nombre,numero,correo));
	}
	
	public Contacto buscarContacto(String nombre) {
		for(int i = 0; i < contactos.size(); i++) {
			if(contactos.get(i).getNombre().equals(nombre))
				return contactos.get(i);
		}
		return null;
	}
	
	public boolean eliminarContacto(String nombre) {
		return contactos.remove(buscarContacto(nombre));
	}
	
	public boolean editarNombre(String nombreNuevo, String nombre) {
		if(buscarContacto(nombre) == null)
			return false;
		buscarContacto(nombre).setNombre(nombreNuevo);
		return true;
	}
	
	public boolean editarNumero(int numeroNuevo, String nombre) {
		if(buscarContacto(nombre) == null)
			return false;
		buscarContacto(nombre).setNumero(numeroNuevo);
		return true;
	}
	
	public boolean editarCorreo(String correoNuevo, String nombre) {
		if(buscarContacto(nombre) == null)
			return false;
		buscarContacto(nombre).setCorreo(correoNuevo);
		return true;
	}
	
	public int contactosTotales() {
		return contactos.size();
	}

}