package classicRPG.model;

import javafx.scene.control.TextArea;

public class TextAreaTester {

	private TextArea ta;
	
	private int enemyHP = 3;
	
	public TextAreaTester() {
		ta = new TextArea();
	}
	
	public void setTextArea(TextArea texta) {
		ta = texta;
	}
	public int getEnemyHP() {
		return enemyHP;
	}
	public void setEnemyHP() {
		enemyHP--;
	}
	
	public void hit(int player) {
		switch (player) {
			case 1: ta.appendText("\nFighter hit you!");
					setEnemyHP();
					break;
			case 2: ta.appendText("\nMage hit you!");
					enemyHP--;
					break;
			case 3: ta.appendText("\nRogue hit you!");
					enemyHP--;
					break;
			case 4: ta.appendText("\nPaladin hit you!");
					enemyHP--;
					break;
		}
	}
	public void defend(int player) {
		switch (player) {
			case 1: ta.appendText("\nFighter defended!");
					break;
			case 2: ta.appendText("\nMage defended!");
					break;
			case 3: ta.appendText("\nRogue defended!");
					break;
			case 4: ta.appendText("\nPaladin defended!");
					break;
		}
	}
	public void magic(int player) {
		switch (player) {
			case 1: ta.appendText("\nFighter magicked you!");
					enemyHP--;
					break;
			case 2: ta.appendText("\nMage magicked you!");
					enemyHP--;
					break;
			case 3: ta.appendText("\nRogue magicked you!");
					enemyHP--;
					break;
			case 4: ta.appendText("\nPaladin magicked you!");
					enemyHP--;
					break;
		}
	}
	
	public void item(int player) {
		switch (player) {
			case 1: ta.appendText("\nFighter item!");
					break;
			case 2: ta.appendText("\nMage item!");
					break;
			case 3: ta.appendText("\nRogue item!");
					break;
			case 4: ta.appendText("\nPaladin item!");
					break;
		}
	}
	
	public void newBattle() {
		enemyHP = 3;
	}
	
	public int whoTurn(String player) {
		if (player.equalsIgnoreCase("Fighter")) return 1;
		else if (player.equalsIgnoreCase("Mage")) return 2;
		else if (player.equalsIgnoreCase("Rogue")) return 3;
		else return 4;
	}
	
	public void listEnemies() {
		ta.appendText("\nDude1");
		ta.appendText("\nDude2");
		ta.appendText("\nDude3");
		ta.appendText("\nDude4");
	}
	
	public void turn(int player, int choice) {		
		while (enemyHP > 0) {
			ta.appendText("\nWhat do?");
			
			switch (player) {
				case 1: switch (choice) {
							case 1: hit(1);
									break;
							case 2: defend(1);
									break;
							case 3: magic(1);
									break;
							case 4: item(1);
							default: ta.appendText("\nNo action taken");
						}
				case 2: switch (choice) {
							case 1: hit(2);
									break;
							case 2: defend(2);
									break;
							case 3: magic(2);
									break;
							case 4: item(2);
					default: ta.appendText("\nNo action taken");
						}
				case 3: switch (choice) {
							case 1: hit(3);
									break;
							case 2: defend(3);
									break;
							case 3: magic(3);
									break;
							case 4: item(3);
							default: ta.appendText("\nNo action taken");
						}
				case 4: switch (choice) {
							case 1: hit(4);
									break;
							case 2: defend(4);
									break;
							case 3: magic(4);
									break;
							case 4: item(4);
							default: ta.appendText("\nNo action taken");
						}
			}
		}			
	}
}