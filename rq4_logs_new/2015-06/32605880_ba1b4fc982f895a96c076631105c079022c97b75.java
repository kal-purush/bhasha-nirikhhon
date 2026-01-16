/**
 *
 * @author r.alvarado
 */

package com.swimcr.customPojo;

public class EntrenamientoPojo {
	private int id;
    private int id_equipo;
    private String fecha;

	public EntrenamientoPojo() {
	}
	
	public EntrenamientoPojo(int id, int id_equipo, String fecha) {
		this.id = id;
		this.id_equipo = id_equipo;
		this.fecha = fecha;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public int getId_equipo() {
		return id_equipo;
	}

	public void setId_equipo(int id_equipo) {
		this.id_equipo = id_equipo;
	}

	public String getFecha() {
		return fecha;
	}

	public void setFecha(String fecha) {
		this.fecha = fecha;
	}
}