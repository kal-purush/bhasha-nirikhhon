public class Dijak {
	private String ime, priimek, predmet;
	private int ocena;
	
	public Dijak(String ime, String priimek, String predmet, int ocena) {
		this.ime = ime;
		this.priimek = priimek;
		this.predmet = predmet;
		this.ocena = ocena;
	}
	
	public String vrniIme() {
		return this.ime;
	}
	
	public String vrniPriimek() {
		return this.priimek;
	}
	
	public String vrniPredmet() {
		return this.predmet;
	}
	
	public int vrniOceno() {
		return this.ocena;
	}
 
	public String toString() {
		String pravilnoIme = this.ime.substring(0, 1).toUpperCase() + this.ime.substring(1).toLowerCase();
		String pravilniPriimek = this.priimek.substring(0, 1).toUpperCase() + this.priimek.substring(1).toLowerCase();
		return  pravilnoIme + " " + pravilniPriimek + " -> " + this.ocena;
	}
}