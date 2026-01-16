package main.dto;

public class PretragaKlinikeDTO {

	String datum;
	String tipPregleda;
	
	public PretragaKlinikeDTO(String datum, String tipPregleda) {
		super();
		this.datum = datum;
		this.tipPregleda = tipPregleda;
	}

	public PretragaKlinikeDTO() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getDatum() {
		return datum;
	}

	public void setDatum(String datum) {
		this.datum = datum;
	}

	public String getTipPregleda() {
		return tipPregleda;
	}

	public void setTipPregleda(String tipPregleda) {
		this.tipPregleda = tipPregleda;
	}
	
}