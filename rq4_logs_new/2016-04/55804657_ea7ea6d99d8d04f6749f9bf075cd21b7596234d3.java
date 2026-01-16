package test;

/*
 * the point of this class is to show a few different ways we can make a die.
 * Ill make this class have a specified number of sides and we can just have slight variations of it for
 * bigger dice. Ill also have 2 different ways we can make it and ill just comment 1 out. not sure which 
 * way is better so we can figure that out.
 * The other die class ill make will just be a die with no size specification so itll take parameters each time.
 */

import java.util.Random;

public class Die6 {
	
	private static Random random = new Random();
	
	public Die6() {
	}
	
	/**
	 * basic roll die method that gets a random number 1-6 and spits it out
	 * @return the result of a random roll from 1-6
	 */
	public int roll() {
		return random.nextInt(6) + 1;
	}
	
	/**
	 * a more complex roll method that we may need depending on the mechanics we end up deciding to use.
	 * rolls the number of dice that you tell it to and returns the largest result of those rolls.
	 * (can also be changed to smallest depending on what we want or we can just make 2 seperate methods)
	 * @param numberOfDice the number of dice to roll
	 * @return the largest(?) of the rolls
	 */
	public int roll(int numberOfDice) {
		int[] results = new int[numberOfDice];
		int largest = 0;
		for (int i = 0; i < numberOfDice; i++) {
			results[i] = roll();
			if (i == 0 || results[i] > largest) {
				largest = results[i];
			}
		}
		return largest;
	}
	
	
	
}