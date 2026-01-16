package jobs;

public class Arcane {

	String nomArcane;
	int degatsArcane;
	Effet effetArcane;
	int coutMana;
	int coutPA;
	int nivAcquisition;
	String description;

	public Arcane(){
		this.nomArcane = "Arcane";
		this.description = "";
	}
	public Arcane(String nomArcane, int degatsArcane,
			Effet effetArcane, int coutMana, int coutPA, int nivAcquisition,
			String description) {
		this.nomArcane = nomArcane;
		this.degatsArcane = degatsArcane;
		this.effetArcane = effetArcane;
		this.coutMana = coutMana;
		this.coutPA = coutPA;
		this.nivAcquisition = nivAcquisition;
		this.description = description;
	}

	public Arcane(Arcane a){
		this.nomArcane = new String(a.getNomArcane());
		this.degatsArcane = a.getDegatsArcane();
		this.effetArcane = a.getEffetArcane();
		this.coutMana = a.getCoutMana();
		this.coutPA = a.getCoutPA();
		this.nivAcquisition = a.getNivAcquisition();
		this.description = a.getDescription();
	}

	public int getCoutMana() {
		return coutMana;
	}

	public void setCoutMana(int coutMana) {
		this.coutMana = coutMana;
	}

	public int getCoutPA() {
		return coutPA;
	}

	public void setCoutPA(int coutPA) {
		this.coutPA = coutPA;
	}

	public int getNivAcquisition() {
		return nivAcquisition;
	}

	public void setNivAcquisition(int nivAcquisition) {
		this.nivAcquisition = nivAcquisition;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	
	public int calculDegatsArcaneMagie(Heros x){
		int degatsDurantCombat = this.degatsArcane + (int)(0.25d)*x.getIntelligence(); 
		return degatsDurantCombat;
	}

	public String getNomArcane() {
		return nomArcane;
	}

	public void setNomArcane(String nomArcane) {
		this.nomArcane = nomArcane;
	}

	public int getDegatsArcane() {
		return degatsArcane;
	}

	public void setDegatsArcane(int degatsArcane) {
		this.degatsArcane = degatsArcane;
	}

	public Effet getEffetArcane() {
		return effetArcane;
	}

	public void setEffetArcane(Effet effetArcane) {
		this.effetArcane = effetArcane;
	}

	public String toString(){
		return nomArcane + " " + degatsArcane + " " + effetArcane + " " +coutMana +" "+ coutPA; 
	}
	
	//public String toStringTableau(){

	//}
	public static void main(String[] args){
		Effet brulure= new Effet("Brulure", 5); 
		Effet estocadeDemoniaque = new Effet("Estocade Demoniaque", 10);
		}
	
}