package polimorfismo.ejemplos;

public class Entrenador extends SeleccionFutbol {
	private int idFederacion;

	public Entrenador(int id, String nombre, String apellido, int edad, int idFederacion) {
		super(id, nombre, apellido, edad);
		this.idFederacion = idFederacion;
	}

	// constructor, getter y setter
	@Override
	public void entrenamiento() {
		System.out.println("Dirige un entrenamiento (Clase Entrenador)");
	}

	@Override
	public void partidoFutbol() {
		System.out.println("Dirige un Partdio (Clase Entrenador)");
	}

	public void planificarEntrenamiento() {
		System.out.println("Planificar un Entrenamiento");
	}

	public int getIdFederacion() {
		return idFederacion;
	}

	public void setIdFederacion(int idFederacion) {
		this.idFederacion = idFederacion;
	}

}