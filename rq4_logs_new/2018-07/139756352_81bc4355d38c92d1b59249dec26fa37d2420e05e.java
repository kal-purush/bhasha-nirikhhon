package polimorfismo.ejemplos;

public class Futbolista extends SeleccionFutbol {
	private int dorsal;
	private String demarcacion;

	public Futbolista(int id, String nombre, String apellido, int edad, int dorsal, String demarcacion) {
		super(id, nombre, apellido, edad);
		this.dorsal = dorsal;
		this.demarcacion = demarcacion;
	}

	// constructor,getter y setter
	@Override
	public void entrenamiento() {
		System.out.println("Realiza un entrenamiento (Clase Futbolista)");
	}

	@Override
	public void partidoFutbol() {
		System.out.println("Juega un partido (Clase Futbolista)");
	}

	public void entrevista() {
		System.out.println("Da una Entrevista");
	}

	/**
	 * @return the dorsal
	 */
	public int getDorsal() {
		return dorsal;
	}

	/**
	 * @param dorsal the dorsal to set
	 */
	public void setDorsal(int dorsal) {
		this.dorsal = dorsal;
	}

	/**
	 * @return the demarcacion
	 */
	public String getDemarcacion() {
		return demarcacion;
	}

	/**
	 * @param demarcacion the demarcacion to set
	 */
	public void setDemarcacion(String demarcacion) {
		this.demarcacion = demarcacion;
	}

}