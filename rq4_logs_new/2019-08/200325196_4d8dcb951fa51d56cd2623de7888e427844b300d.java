package co.edu.eam.ingesoft.stores_ms.model;

import java.io.Serializable;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class Stores implements Serializable {

	@Id
	private String id;
	
	private String name;
	
	private Long lat;
	
	private Long lng;
	
	private String dscripcion;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getLat() {
		return lat;
	}

	public void setLat(Long lat) {
		this.lat = lat;
	}

	public Long getLng() {
		return lng;
	}

	public void setLng(Long lng) {
		this.lng = lng;
	}

	public String getDscripcion() {
		return dscripcion;
	}

	public void setDscripcion(String dscripcion) {
		this.dscripcion = dscripcion;
	}
	
	
}