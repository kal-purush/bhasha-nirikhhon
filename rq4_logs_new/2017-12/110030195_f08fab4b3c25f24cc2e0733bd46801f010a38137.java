import java.util.ArrayList;

import enumeration.Couleur;

public class GroupePierre {
	
	private Couleur couleur ;
	private ArrayList<Pierre> listPierre = new ArrayList<Pierre>(); 
	private boolean libre;
	private ArrayList<Pierre> listGroupe; 
	
	
	GroupePierre (Couleur c ){
		
		libre = true; 
		couleur = c ;
		}
	
	
	
	/**
	 * 
	 * Getters et Setters 
	 * 
	 */
	public Couleur getCouleur() {
		return couleur;
	}

	public void setCouleur(Couleur couleur) {
		this.couleur = couleur;
	}

	public ArrayList<Pierre> getListGroupe() {
		return listGroupe;
	}

	public void setListGroupe(ArrayList<Pierre> listGroupe) {
		this.listGroupe = listGroupe;
	}

	public boolean isLibre() {
		return libre;
	}

	public void setLibre(boolean libre) {
		this.libre = libre;
	}



	public ArrayList<Pierre> getListPierre() {
		return listPierre;
	}



	public void setListPierre(ArrayList<Pierre> listPierre) {
		this.listPierre = listPierre;
	}

	

}