package main.dto;

public class PretragaLekaraDTO {
	
		String ime;
		String prezime;
		Double ocena;
		public PretragaLekaraDTO() {
			super();
			// TODO Auto-generated constructor stub
		}
		public PretragaLekaraDTO(String ime, String prezime, Double ocena) {
			super();
			this.ime = ime;
			this.prezime = prezime;
			this.ocena = ocena;
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
		public Double getOcena() {
			return ocena;
		}
		public void setOcena(Double ocena) {
			this.ocena = ocena;
		}
		
		
}
