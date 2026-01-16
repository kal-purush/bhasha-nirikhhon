package main.dto;

public class PretragaPacijentaDTO {
	
	String ime;
	String prezime;
	String jmbg;
	
	public PretragaPacijentaDTO(String ime, String prezime, String jmbg) {
		super();
		this.ime = ime;
		this.prezime = prezime;
		this.jmbg = jmbg;
	}
	public PretragaPacijentaDTO() {
		super();
		// TODO Auto-generated constructor stub
	}
	public String getIme() {
		return ime;
	}
	public void setIme(String ime) {
		this.ime = ime;
	}
	public String getPrezime() {
		return prezime;
	}
	public void setPrezime(String prezime) {
		this.prezime = prezime;
	}
	public String getJmbg() {
		return jmbg;
	}
	public void setJmbg(String jmbg) {
		this.jmbg = jmbg;
	}
	



}