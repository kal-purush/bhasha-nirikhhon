package ar.edu.unju.edm.model;

public class Pregunta {
	
	private int codPregunta; //clave primaria
	private String enunciado;
	private int nivel;
	private String opcion1;
	private String opcion2;
	private String opcion3;
	private String opcion4;
	private int opcionCorrecta;
	private double puntaje;
	
	public Pregunta() {
		// TODO Auto-generated constructor stub
	}

	public Pregunta(int codPregunta, String enunciado, int nivel, String opcion1, String opcion2, String opcion3,
			String opcion4, int opcionCorrecta, double puntaje) {
		super();
		this.codPregunta = codPregunta;
		this.enunciado = enunciado;
		this.nivel = nivel;
		this.opcion1 = opcion1;
		this.opcion2 = opcion2;
		this.opcion3 = opcion3;
		this.opcion4 = opcion4;
		this.opcionCorrecta = opcionCorrecta;
		this.puntaje = puntaje;
	}

	public int getCodPregunta() {
		return codPregunta;
	}

	public void setCodPregunta(int codPregunta) {
		this.codPregunta = codPregunta;
	}

	public String getEnunciado() {
		return enunciado;
	}

	public void setEnunciado(String enunciado) {
		this.enunciado = enunciado;
	}

	public int getNivel() {
		return nivel;
	}

	public void setNivel(int nivel) {
		this.nivel = nivel;
	}

	public String getOpcion1() {
		return opcion1;
	}

	public void setOpcion1(String opcion1) {
		this.opcion1 = opcion1;
	}

	public String getOpcion2() {
		return opcion2;
	}

	public void setOpcion2(String opcion2) {
		this.opcion2 = opcion2;
	}

	public String getOpcion3() {
		return opcion3;
	}

	public void setOpcion3(String opcion3) {
		this.opcion3 = opcion3;
	}

	public String getOpcion4() {
		return opcion4;
	}

	public void setOpcion4(String opcion4) {
		this.opcion4 = opcion4;
	}

	public int getOpcionCorrecta() {
		return opcionCorrecta;
	}

	public void setOpcionCorrecta(int opcionCorrecta) {
		this.opcionCorrecta = opcionCorrecta;
	}

	public double getPuntaje() {
		return puntaje;
	}

	public void setPuntaje(double puntaje) {
		this.puntaje = puntaje;
	}

	@Override
	public String toString() {
		return "Pregunta [codPregunta=" + codPregunta + ", enunciado=" + enunciado + ", nivel=" + nivel + ", opcion1="
				+ opcion1 + ", opcion2=" + opcion2 + ", opcion3=" + opcion3 + ", opcion4=" + opcion4
				+ ", opcionCorrecta=" + opcionCorrecta + ", puntaje=" + puntaje + "]";
	}
	
	
}