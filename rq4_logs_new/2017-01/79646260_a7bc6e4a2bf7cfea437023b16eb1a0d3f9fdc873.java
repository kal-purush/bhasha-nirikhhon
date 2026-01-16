
public class Skills {
	private int acrobatics;
	private int animalHandling;
	private int arcana;
	private int athletics;
	private int deception;
	private int history;
	private int insight;
	private int intimidation;
	private int investigation;
	private int medicine;
	private int nature;
	private int perception;
	private int performance;
	private int persuasion;
	private int religion;
	private int slightOfHand;
	private int stealth;
	private int survival;
	private Character character;
	
	
	public int getAcrobatics() {
		return acrobatics;
	}
	public void setAcrobatics(int acrobatics) {
		this.acrobatics = acrobatics;
	}
	public void updateAcrobatics() {
		int res = (character.getDex() - 10) / 2;
		acrobatics += res;
	}
	public int getAnimalHandling() {
		return animalHandling;
	}
	public void setAnimalHandling(int animalHandling) {
		this.animalHandling = animalHandling;
	}
	public void updateAnimalHandling() {
		int res = (character.getWis() - 10) / 2;
		animalHandling += res;
	}
	
	public int getArcana() {
		return arcana;
	}
	public void setArcana(int arcana) {
		this.arcana = arcana;
	}
	public void updateArcana() {
		int res = (character.getIntelli() - 10) / 2;
		arcana += res;
	}
	public int getAthletics() {
		return athletics;
	}
	public void setAthletics(int athletics) {
		this.athletics = athletics;
	}
	public void updateAthletics() {
		int res = (character.getStr() - 10) / 2;
		athletics += res;
	}
		
	public int getDeception() {
		return deception;
	}
	public void setDeception(int deception) {
		this.deception = deception;
	}
	public void updateDeception() {
		int res = (character.getCharisma() - 10) / 2;
		deception += res;
	}
	public int getHistory() {
		return history;
	}
	public void setHistory(int history) {
		this.history = history;
	}
	public void updateHistory() {
		int res = (character.getIntelli() - 10) / 2;
		history += res;
	}
	public int getInsight() {
		return insight;
	}
	public void setInsight(int insight) {
		this.insight = insight;
	}
	public void updateInsight() {
		int res = (character.getWis() - 10) / 2;
		insight += res;
	}
	public int getIntimidation() {
		return intimidation;
	}
	public void setIntimidation(int intimidation) {
		this.intimidation = intimidation;
	}
	public void updateIntimidation() {
		int res = (character.getCharisma() - 10) / 2;
		intimidation += res;
	}
	public int getInvestigation() {
		return investigation;
	}
	public void setInvestigation(int investigation) {
		this.investigation = investigation;
	}
	public void updateInvestigation() {
		int res = (character.getIntelli() - 10) / 2;
		investigation += res;
	}
	public int getMedicine() {
		return medicine;
	}
	public void setMedicine(int medicine) {
		this.medicine = medicine;
	}
	public void updateMedicine() {
		int res = (character.getWis() - 10) / 2;
		medicine += res;
	}
	public int getNature() {
		return nature;
	}
	public void setNature(int nature) {
		this.nature = nature;
	}
	public void updateNature() {
		int res = (character.getIntelli() - 10) / 2;
		nature += res;
	}
	public int getPerception() {
		return perception;
	}
	public void setPerception(int perception) {
		this.perception = perception;
	}
	public void updatePerception() {
		int res = (character.getWis() - 10) / 2;
		perception += res;
	}
	public int getPerformance() {
		return performance;
	}
	public void setPerformance(int performance) {
		this.performance = performance;
	}
	public void updatePerformance() {
		int res = (character.getCharisma() - 10) / 2;
		performance += res;
	}
	public int getPersuasion() {
		return persuasion;
	}
	public void setPersuasion(int persuasion) {
		this.persuasion = persuasion;
	}
	public void updatePersuasion() {
		int res = (character.getCharisma() - 10) / 2;
		persuasion += res;
	}
	public int getReligion() {
		return religion;
	}
	public void setReligion(int religion) {
		this.religion = religion;
	}
	public void updateReligion() {
		int res = (character.getIntelli() - 10) / 2;
		religion += res;
	}
	public int getSlightOfHand() {
		return slightOfHand;
	}
	public void setSlightOfHand(int slightOfHand) {
		this.slightOfHand = slightOfHand;
	}
	public void updateSleightOfHand() {
		int res = (character.getDex() - 10) / 2;
		slightOfHand += res;
	}
	public int getStealth() {
		return stealth;
	}
	public void setStealth(int stealth) {
		this.stealth = stealth;
	}
	public void updateStealth() {
		int res = (character.getDex() - 10) / 2;
		stealth += res;
	}
	public int getSurvival() {
		return survival;
	}
	public void setSurvival(int survival) {
		this.survival = survival;
	}
	public void updateSurvival() {
		int res = (character.getWis() - 10) / 2;
		survival += res;
	}
	
	
	
	
}