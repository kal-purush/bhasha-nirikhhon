package ar.edu.unlam.practica;

public class Auto extends Vehiculo{

	public Auto(String patente, Coordenada posicion) {
		super(patente, posicion);
		
	}
	
	

	@Override
	public int compareTo(Vehiculo o) {
		return this.getPatente().compareTo(o.getPatente());
	}



}