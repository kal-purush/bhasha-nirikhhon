package ichim.proiect.cts;

public class Client {
	
	private String nume;
	private int varsta;
	private String telefon;
	private String email;
	private boolean esteFidel;
	private String adresa;
	private MetodaPlata metodaPlata;
	private Comanda comanda;

	public String getNume() {
		return nume;
	}

	public void setNume(String nume) {
		this.nume = nume;
	}

	public int getVarsta() {
		return varsta;
	}

	public void setVarsta(int varsta) {
		this.varsta = varsta;
	}

	public String getTelefon() {
		return telefon;
	}

	public void setTelefon(String telefon) {
		this.telefon = telefon;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public boolean isEsteFidel() {
		return esteFidel;
	}

	public void setEsteFidel(boolean esteFidel) {
		this.esteFidel = esteFidel;
	}

	public String getAdresa() {
		return adresa;
	}

	public void setAdresa(String adresa) {
		this.adresa = adresa;
	}

	public MetodaPlata getMetodaPlata() {
		return metodaPlata;
	}

	public void setMetodaPlata(MetodaPlata metodaPlata) {
		this.metodaPlata = metodaPlata;
	}

	public Comanda getComanda() {
		return comanda;
	}

	public void setComanda(Comanda comanda) {
		this.comanda = comanda;
	}

	public void achita(){
		
	}
}