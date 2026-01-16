package lab.eci.ocupados.entites;

public class Computador {

	String nombre;
	int id;

	public Computador(String nombre, int id) {
		this.nombre = nombre;
		this.id = id;
	}

	public Computador() {

	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return "Computador{" + "nombre=" + nombre + ", id=" + id + '}';
	}

}