package hunter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import core.Environnement;
import core.Position;

class CaseLabyrinthe {
	List<CaseLabyrinthe> memeChemin;
	Environnement environnement;
	public Position positionEnv;
	public Position positionLab;
	public int identifiant;
	
	public CaseLabyrinthe(Environnement environnement, Position positionEnv, Position positionLab,int identifiant) {
		this.memeChemin = new ArrayList<CaseLabyrinthe>();
		this.environnement = environnement;
		this.positionEnv = positionEnv;
		this.positionLab = positionLab;
		this.identifiant = identifiant;
	}
	
	public Position getMurNord() {
		if(positionEnv.getPositionY() == 0) {
			return null;
		}
		return new Position(positionEnv.getPositionX(), positionEnv.getPositionY()-1);
	}
	
	public Position getMurSud() {
		if(positionEnv.getPositionY() == environnement.getGridSizeY()-1) {
			return null;
		}
		return new Position(positionEnv.getPositionX(), positionEnv.getPositionY()+1);
	}
	
	public Position getMurOuest() {
		if(positionEnv.getPositionX() == 0) {
			return null;
		}
		return new Position(positionEnv.getPositionX()-1, positionEnv.getPositionY());
	}
	
	public Position getMurEst() {
		if(positionEnv.getPositionX() == environnement.getGridSizeX()-1) {
			return null;
		}
		return new Position(positionEnv.getPositionX()+1, positionEnv.getPositionY());
	}
	
	public boolean murEstOuvert(Position position) {
		if(environnement.getEnvironnement()[position.getPositionX()][position.getPositionY()] == null) {
			return true;
		}
		return false;
	}
	
	public boolean estDansLeMemeChemin(CaseLabyrinthe cases) {
		return this.identifiant == cases.identifiant;
	}
	
	public void modifierIdentifiantChemin(int identifiant) {
		for(int i = 0; i < memeChemin.size(); i++) {
			memeChemin.get(i).identifiant = identifiant;
		}
	}
	
	public Position getMurAleatoire() {
		Position position = null;
		do {
			Random r = new Random();
			int mur = r.nextInt(4);
			if(mur == 0) {
				position = getMurNord();
			}else if(mur == 1) {
				position = getMurSud();
			}else if (mur == 3) {
				position = getMurOuest();
			}else if (mur == 4) {
				position = getMurEst();
			}
		}while(position == null);
		return position;
	}
}