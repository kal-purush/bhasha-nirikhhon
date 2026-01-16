package Napakalaki;

/**
 *
 * @author jesusjimsa & jaimefriass
 */
public class Cultist {
	private String name;
	private int gainedLevels;
	
	public Cultist(String name, int gainedLevels){
		this.name = name;
		this.gainedLevels = gainedLevels;
	}
	
	int getGainedLevels(){
		return gainedLevels;
	}
	
}