package polimorfismo.ejemplos;

public class Masajista extends SeleccionFutbol {
	private String titulacion;
	private int aniosExperiencia;

	public Masajista(int id, String nombre, String apellido, int edad, String titulacion, int experiencia) {
		super(id, nombre, apellido, edad);
		this.titulacion = titulacion;
		this.aniosExperiencia = experiencia;
	}

	// constructor, getter y setter
	@Override
	public void entrenamiento() {
		System.out.println("Da asistencia en el entrenamiento (Clase Masajista)");
	}

	/**
	 * @return the titulacion
	 */
	public String getTitulacion() {
		return titulacion;
	}

	/**
	 * @param titulacion the titulacion to set
	 */
	public void setTitulacion(String titulacion) {
		this.titulacion = titulacion;
	}

	/**
	 * @return the aniosExperiencia
	 */
	public int getAniosExperiencia() {
		return aniosExperiencia;
	}

	/**
	 * @param aniosExperiencia the aniosExperiencia to set
	 */
	public void setAniosExperiencia(int aniosExperiencia) {
		this.aniosExperiencia = aniosExperiencia;
	}
}