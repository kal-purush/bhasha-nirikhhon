package Entities;

import java.util.ArrayList;

import Structures.Lieu;

public class Chien extends Personnage {
	//Attributs
	
	//Constructeurs	
	public Chien(Lieu lieu, int nbActions) {
		super(lieu, nbActions);
	}

	//Méthodes
	public void action() { // Chaque action nbActions - 1
		
	}
	@Override
	public ArrayList<Lieu> lieuxAccessibles() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void nextDay() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void update() {
		// TODO Auto-generated method stub
		
	}
}