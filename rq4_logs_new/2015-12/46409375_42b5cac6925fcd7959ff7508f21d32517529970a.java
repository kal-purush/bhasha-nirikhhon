package fr.univ.tlse2.sfr.server;

public class Carte {
	private String nom_carte;
	private int largeur;
	private int hauteur;
	
	public Carte(){
		this.nom_carte = "Carte par d�faut";
		this.largeur = 2;
		this.hauteur = 2;
	}
	
	public Carte(String nom, int p_largeur, int p_hauteur){
		this.nom_carte = nom;
		this.largeur = p_largeur;
		this.hauteur = p_hauteur;
	}
}