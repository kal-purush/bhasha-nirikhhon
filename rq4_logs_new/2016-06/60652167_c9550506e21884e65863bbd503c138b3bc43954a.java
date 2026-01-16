package beans;

public class Adresse {
	private long id;
	private int numero;
	private String rue;
	private String ville;
	private Integer codePostal;
	
	public Adresse() {}
	
	public Adresse(long id, int numero,String rue, String ville, int codePostal) {
		this.id = id;
		this.numero = numero;
		this.rue = rue;
		this.ville = ville;
		this.codePostal = codePostal;
	}
	
	public long getId() {
        return id;
    }
	
	public Adresse setId(long id) {
        this.id = id;
        return this;
    }	
	
	public long getNumero() {
        return numero;
    }
	
	public Adresse setNumero(int numero) {
        this.numero = numero;
        return this;
    }

	public String getRue() {
        return rue;
    }
	
	public Adresse setRue(String rue) {
        this.rue = rue;
        return this;
    }
	
	public String getVille() {
        return ville;
    }
	public Adresse setVille(String ville) {
        this.ville = ville;
        return this;
    }

	public int getCodePostal() {
        return codePostal;
    }
	public Adresse setCodePostal(int codePostal) {
        this.codePostal = codePostal;
        return this;
    }
}