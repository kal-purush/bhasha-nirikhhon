package Structures;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Routes {
	private Map<Lieu, ArrayList<Lieu>> connexion;
	
	public Routes(ArrayList<Lieu> lieus) {
		this.connexion = new HashMap<Lieu, ArrayList<Lieu>>();
		for(int i = 0; i < lieus.size(); i++) {
			 connexion.put(lieus.get(i), new ArrayList<>());
		}
	}
	
	public Map<Lieu, ArrayList<Lieu>> getConnexion() {
		return this.connexion;
	}
	
	public void setConnexion(Map<Lieu,ArrayList<Lieu>> connexion) {
		this.connexion = connexion;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Routes other = (Routes) obj;
		if (connexion == null) {
			if (other.connexion != null)
				return false;
		} else if (!connexion.equals(other.connexion))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Routes [connexion=" + connexion + "]";
	}
	
	
	/**
	 * Permet d'ajouter <b>un</b> élément dans la liste des voisins d'un lieu
	 * @param lieu auquel vous voulez <b>ajouter</b> un voisin
	 * @param <b>voisin</b> que vous voulez ajouter à l'ArrayList
	 * @return <b>vrai</b> ou <b>faux</b> si l'ajout à reussi ou pas (c'est à dire
	 * si la clé de la HashMap existe)
	 */
	public boolean addConnexion(Lieu lieu, Lieu voisin) {
		return this.connexion.get(lieu).add(voisin);
	}
	
	/**
	 * Permet d'ajouter <b>plusieurs</b> éléments dans la liste des voisins d'un lieu
	 * <b>sans supprimer</b> les voisins existant.
	 * @param lieu auquel vous voulez <b>ajouter</b> des voisins
	 * @param <b>voisins</b> que vous voulez ajouter à l'ArrayList
	 * @return <b>vrai</b> ou <b>faux</b> si l'ajout à reussi ou pas (c'est à dire
	 * si la clé de la HashMap existe)
	 */
	public boolean addConnexion(Lieu lieu, ArrayList<Lieu> voisins) {
		return this.connexion.get(lieu).addAll(voisins);
	}
	
	/**
	 * Permet de savoir si un lieu possède un voisin <b>fournit en paramètre</b>
	 * @param lieu
	 * @param voisin
	 * @return <b>vrai</b> ou <b>faux</b> si le lieu possède le voisin ou pas.
	 */
	public boolean isConnected(Lieu lieu, Lieu voisin) {
		return this.connexion.get(lieu).contains(voisin);
	}
	
	/**
	 * Permet de savoir si un lieu possède un ou plusieurs voisin(s)
	 * @param lieu
	 * @return retourne <b>vrai</b> si le lieu possède au moins un voisin;
	 */
	public boolean haveVoisin(Lieu lieu) {
		return !this.connexion.get(lieu).isEmpty();
	}
	
	/**
	 * <b>Supprime</b> le voisin d'un lieu fournit en paramètre
	 * @param lieu pour lequel on veut ajouter un voisin
	 * @param voisin que l'on veut ajouter
	 * @return <b>vrai</b> si le voisin a pu être retiré (c'est à dire, si le voisin
	 * fournit en paramètre est vraiment voisin avec le lieu
	 */
	public boolean removeVoisin(Lieu lieu, Lieu voisin) {
		return this.connexion.get(lieu).remove(voisin);
	}
	
	/**
	 * Retire tous les éléments de l'ArrayList du lieu
	 * @param lieu
	 */
	public void removeAllVoisin(Lieu lieu) {
		this.connexion.remove(lieu);
		this.connexion.put(lieu, new ArrayList<>());
	}

	
	
	
	
}