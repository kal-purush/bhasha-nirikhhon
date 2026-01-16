package com.adventuresof.game.combat;

import java.util.Random;

import com.adventuresof.game.character.GameCharacter;

public class DamageCalculator {

	public static Random random = new Random();
	
	public static int calculateDamage(int maxDamage, GameCharacter inflictedBy, GameCharacter inflictedOn) {
		// total up the total damage points (for attacker) and add to max damage
		int totalDamage = maxDamage + inflictedBy.getDamageBonusPoints() + inflictedBy.getTemporaryDamageBonusPoints();
		// total up total defence and health points of defender
		int totalDefencePoints = inflictedOn.getDefenceBonusPoints();
		// calculate damage amount - generate a number between 1-100 representing a % of the max damage to deal
		double calculatedRandomDamage = totalDamage * ((double)random.nextInt(100)/100);
		
		// return the calculated % damage value - the total defence points
		return (int) calculatedRandomDamage - totalDefencePoints;
	}
	
}