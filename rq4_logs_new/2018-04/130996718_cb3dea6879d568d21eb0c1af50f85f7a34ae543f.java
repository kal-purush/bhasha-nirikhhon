package com.ecetech.bti3.projetIT.scpi.beans;

public class PartSCPI {
	
	private int id;
	private String SCPI;
	private double nombre;
	private double valeur;
	private int statut;
	private String compte;
	
	public PartSCPI(int _id,String _scpi, Double _nombre, Double _valeur, int _statut, String _compte){
		this.id = _id;
		this.SCPI = _scpi;
		this.nombre = _nombre;
		this.valeur = _valeur;
		this.statut = _statut;
		this.compte = _compte;
	}
	
	public PartSCPI(){
		this(0,"",0.0,0.0,0,"");
	}
	
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getSCPI() {
		return SCPI;
	}
	public void setSCPI(String sCPI) {
		SCPI = sCPI;
	}
	public double getNombre() {
		return nombre;
	}
	public void setNombre(Double nombre) {
		this.nombre = nombre;
	}
	public double getValeur() {
		return valeur;
	}
	public void setValeur(Double valeur) {
		this.valeur = valeur;
	}
	public int getStatut() {
		return statut;
	}
	public void setStatut(int statut) {
		this.statut = statut;
	}
	
	public String getCompte() {
		return compte;
	}

	public void setCompte(String compte) {
		this.compte = compte;
	}

	@Override
	public String toString() {
		return "Part [SCPI=" + SCPI + ", nombre=" + nombre + ", valeur="
				+ valeur + ", statut=" + statut + ", compte=" + compte + "]";
	}
	
	
}