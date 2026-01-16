package Models;

public class EtatCarte {

	private String nom_carte;
	private int largeur;
	private int hauteur;
	
	public EtatCarte(){
		this.nom_carte = new String();
		this.largeur = 0;
		this.hauteur = 0;
	}
	
	public EtatCarte(String nom, int p_largeur, int p_hauteur){
		this.nom_carte = nom;
		this.largeur = p_largeur;
		this.hauteur = p_hauteur;
	}

	public String getNom_carte() {
		return nom_carte;
	}

	public void setNom_carte(String nom_carte) {
		this.nom_carte = nom_carte;
	}

	public int getLargeur() {
		return largeur;
	}

	public void setLargeur(int largeur) {
		this.largeur = largeur;
	}

	public int getHauteur() {
		return hauteur;
	}

	public void setHauteur(int hauteur) {
		this.hauteur = hauteur;
	}
	
	
}