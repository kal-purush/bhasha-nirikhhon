package uo.sdi.model;


public class ListaApuntados {
	
	public enum PeticionEstado {
		ACCEPTED,
		EXCLUDED,
		PENDANT,
		PROMOTER,
		NO_SEAT
	}

	Trip viaje;
	User usuario;
	Seat asiento;
	PeticionEstado relacionViaje;
	
	public Trip getViaje() {
		return viaje;
	}
	public void setViaje(Trip viaje) {
		this.viaje = viaje;
	}
	public User getUsuario() {
		return usuario;
	}
	public void setUsuario(User usuario) {
		this.usuario = usuario;
	}
	public Seat getAsiento() {
		return asiento;
	}
	public void setAsiento(Seat asiento) {
		this.asiento = asiento;
	}
	public PeticionEstado getRelacionViaje() {
		return relacionViaje;
	}
	public void setRelacionViaje() {
		if(usuario.getId().equals(viaje.getId()))
			this.relacionViaje = PeticionEstado.PROMOTER;
		else if(asiento == null)
			this.relacionViaje = PeticionEstado.PENDANT;
		else if(asiento.getStatus().equals(SeatStatus.ACCEPTED))
			this.relacionViaje = PeticionEstado.ACCEPTED;
		else if(asiento.getStatus().equals(SeatStatus.EXCLUDED))
			this.relacionViaje = PeticionEstado.EXCLUDED;

	}
}