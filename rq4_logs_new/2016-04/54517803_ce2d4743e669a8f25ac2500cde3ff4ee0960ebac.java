package main;

import java.awt.Graphics2D;

import guns.Gun;
import persons.Player;

public class Scoring {
	
	//Increments until greate than maxTicks, then spawns zombie and resets
	//Only used for zaombie spawning
	public static int ticks = 75;
	public static int maxTicks = 150;
	//Zombie stuff
	public static int spawnCount = 0;
	public static int roundCount = 1;
	
	
	//Every 150 rounds, it resets to 0
	// Score and points both incrememt when 150
	public static int scoreCount = 0;
	public static int points = 0;
	//Score to 0 when
	public static int score = 0;
	
	
	
	public static void scoreUpdate(Player player, SupplyDrop supplyDrop) {
		scoreCount++;
		ticks++;
		
		ifPlayerHealthBoost(player);
		
		scoreCountReset();
		
		supplyDropUpdate(supplyDrop, player);
	}
	
	private static void scoreCountReset() {
		if (scoreCount == 150) {
			scoreCount = 0;
			score++;
			points++;
		}
	}
	
	public static void ifPlayerHealthBoost(Player player) {
		if (scoreCount % 25 == 0) {
			player.boostHealth();
		}
	}
	
	public static void supplyDropUpdate(SupplyDrop supplyDrop, Player player) {
		if (score >= 100) {
			score = 0;
			roundCount += 1;
			supplyDrop.spawnSupplyDrop(player);
		}
	}
	
	public static boolean readyToSpawnZombie() {
		if (ticks >= maxTicks) {
			ticks = 0;
			return true;
		}
		return false;
	}
	
	public static int spawnCountAdjustments(int zombieLives) {
		spawnCount++;
		if (maxTicks > 50 && spawnCount % 2 == 0) {
			maxTicks--;
		}
		if (spawnCount == 100 || spawnCount == 50)
			zombieLives += 100;
		return zombieLives;
	}

	public static void scoreToZero() {
		score = 0;		
	}

	public static void gunUpdateScore(Gun currentGun) {
		points += currentGun.score/2;
		score += currentGun.score;
	}
	
	public static void draw(Graphics2D g) {
		g.drawString("Score: " + String.valueOf(Scoring.points), 20, 460);

	}
	
}