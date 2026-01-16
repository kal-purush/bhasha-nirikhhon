package classes;

import java.util.ArrayList;

import Entities.Enqueteur;
import Structures.Lieu;
import Structures.Village;

/**
 * Classe du rôle Maître-Chien (Enquêteur)
 * @author calamar
 *
 */
public class MaitreChien extends Enqueteur {
	//Attributs
	
	//Constructeurs
	public MaitreChien(Village village, int nbActions) {
		super(village, nbActions);
	}
	public MaitreChien(Lieu lieu, int nbActions) {
		super(lieu, nbActions);
	}
	
	//Méthodes
	@Override
	public ArrayList<Lieu> lieuxAccessibles() {
		return lieu.getVoisins();
	}

	@Override
	public void nextDay() {
		goTo(lieu);
		
	}

	@Override
	public void update() {
		nextDay();
		
	}
	
}