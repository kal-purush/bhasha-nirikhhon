package pg;

import java.util.ArrayList;

public class Pendu {
	
	private String motMystere;
	private StringBuilder motATrous;
	private int compteur;
	private int maxEchecs = 5;
	private ArrayList<String> erreurs = new ArrayList<String>();
	
	public Pendu() {
		motMystere = "polymorphisme";
		//au démarrage, le motATrous contient un caractère spécial répété length fois
		motATrous = new StringBuilder("?".repeat(motMystere.length()));
		compteur = 0;
	}
	
	//ces bons vieux getters/setters
	public String getMotMystere() {
		return motMystere;
	}

	public void setMotMystere(String motMystere) {
		this.motMystere = motMystere;
	}

	public StringBuilder getMotATrous() {
		return motATrous;
	}

	public void setMotATrous(StringBuilder motATrous) {
		this.motATrous = motATrous;
	}

	public int getCompteur() {
		return compteur;
	}

	public void setCompteur(int compteur) {
		this.compteur = compteur;
	}

	public int getMaxEchecs() {
		return maxEchecs;
	}

	public void setMaxEchecs(int maxEchecs) {
		this.maxEchecs = maxEchecs;
	}
	
	public ArrayList<String> getErreurs() {
		return erreurs;
	}

	public void setErreurs(ArrayList<String> erreurs) {
		this.erreurs = erreurs;
	}

	//le joueur joue quelque chose
	//on renvoie true si la partie est finie
	//sinon false
	public boolean jouer(String essai) {
		if(essai.length() == 1) {
			jouerLettre(essai.charAt(0));
		}else {
			jouerMot(essai);
		}
		return endGame();
	}
	
	/* Si la lettre jouée est trouvée dans motMystere
	 * on remplace les caractères équivalents dans motATrous
	 */
	public void jouerLettre(char essaiL) {
		//précaution : on travaille tout en lowercase pour éviter les problèmes de casse
		essaiL = Character.toLowerCase(essaiL);
		//variable locale pour déterminer si on a ou non trouvé au moins une occurence de la lettre cherchée
		boolean trouve = false;
		//pour chaque caractère dans le motMystere
		for(int i=0;i<motMystere.length();i++) {
			//si le caractère est celui que je cherche
			if(motMystere.charAt(i) == essaiL) {
				//on remplace le caractère équivalent dans motATrous
				motATrous.setCharAt(i, essaiL);
				trouve = true;
			}
		}
		if(!trouve) {
			//si on a rien trouvé c'est un coup raté
			erreurs.add(Character.toString(essaiL));
			compteur++;
		}
	}
	
	/* Si le mot joué est égal (en ignorant la casse)
	 * au motMystere on remplace entièrement le mot à trous
	 * 
	 */
	public void jouerMot(String essaiM) {
		if(essaiM.equalsIgnoreCase(motMystere)) {
			motATrous.replace(0, motATrous.length(), motMystere);
		}else {
			erreurs.add(essaiM);
			compteur++;
			//TODO : ajouter le mot à la liste d'erreurs
		}
	}
	
	/* Vérifie si l'utilisateur a gagné la partie
	 * si le contenu du motATrous est égal au motMystere
	 * partie gagnée
	 */
	public boolean isWon() {
		if(motMystere.contentEquals(motATrous)) {
			return true;
		}else {
			return false;
		}
	}
	
	/* Vérifie si l'utilisateur a perdu la partie
	 * si le compteur dépasse le nombre max d'échecs autorisés
	 * game over
	 */
	public boolean isLost() {
		if(compteur>maxEchecs) {
			return true;
		}else {
			return false;
		}
	}
	
	/* si fin de partie : renvoie true
	 * sinon renvoie false
	 */
	public boolean endGame() {
		if(isWon() || isLost()) {
			return true;
		}else {
			return false;
		}
	}
	
	//TODO fonction verifierLettreJouee
	
}