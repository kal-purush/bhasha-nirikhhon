package Koulu2;
//Create class Enemy
public class Enemy {
	private int wounds;
	private int tb;
	private int armor;
	private String name;

		
	public Enemy() {
		//In this test phase, only one kind of enemy is possible.
		wounds = 12;
		tb = 6;
		armor = 2;
	}
	// constructor with class name given as string
	// correct attributes are given based on class name
	public Enemy(String classname) {
		name = classname;
		switch(name) {
			case "orkboy":
				wounds = 12;
				tb = 6;
				armor = 2;
				break;
			case "stormboy":
				wounds = 14;
				tb = 6;
				armor = 2;
				break;
			case "orknob":
				wounds = 25;
				tb = 9;
				armor = 2;
				break;
		
		}
	}
		
	public void subWounds(int damage) {
		//After taking damage, calculate current wounds
		wounds =-  damage - tb;
		//calculations are incorrect, output is very wrong
	}
	public int getWounds() {
		return wounds;
	}
}
