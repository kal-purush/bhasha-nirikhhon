package com.bridgelabz.gambling;

import java.util.Random;

public class GamblingSimulator {

	public static final int initial_Stake = 100;
	public static final int stake_Bet = 1;

	public static void main(String[] args) {
		System.out.println("Welcome to the Gambling Simulation Program");

		int totalStake = 0;

		Random random = new Random();
		int game = random.nextInt(2);
		if (game == 1) {
			totalStake = totalStake + stake_Bet;

			System.out.println("Gambler won the bet and have total stake:" + totalStake);
		} else {
			totalStake = totalStake - stake_Bet;

			System.out.println("Gambler lost the bet and have total stake:" + totalStake);
		}

	}
}