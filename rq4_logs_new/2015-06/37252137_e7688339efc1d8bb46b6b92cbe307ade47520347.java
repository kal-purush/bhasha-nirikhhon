package backend;

import java.io.Serializable;

public class Region implements Serializable {

	private static final long serialVersionUID = 3104702860727863157L;
	
	private String name;
	private double lat;
	private double lng;
	
	public Region() {
		super();
	}

	public Region(String name, double lat, double lng) {
		super();
		this.name = name;
		this.lat = lat;
		this.lng = lng;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLng() {
		return lng;
	}

	public void setLng(double lng) {
		this.lng = lng;
	}
	
	@Override
	public String toString() {
		return name + "; " + lat + "; " + lng;
	}
	
}