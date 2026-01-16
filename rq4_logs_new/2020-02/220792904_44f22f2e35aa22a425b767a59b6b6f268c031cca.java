package main.dto;

public class PretragaLekaraDTO {
	
	String ime;
	String prezime;

	public PretragaLekaraDTO(String ime, String prezime) {
		super();
		this.ime = ime;
		this.prezime = prezime;
	}
	public PretragaLekaraDTO() {
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
	
}