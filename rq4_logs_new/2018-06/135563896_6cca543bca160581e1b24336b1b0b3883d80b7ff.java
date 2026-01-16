package Applicatie;

import java.util.Random;
import java.util.Scanner;

public class Applicatie {
	static String[] bord = new String[12];
	static int beurt = 0;
	static Scanner sc = new Scanner(System.in);
	static String antwoord = "";
	static boolean heeftGewonnen = false;

	public static void main(String[] args) {
		maakBord();
		genereerAntwoord();
		while (!heeftGewonnen && beurt < 12) {
			beurt();
			print();
		}
		if (heeftGewonnen == true) {
			System.out.println("Code gekraakt! Het antwoord = " + antwoord);
		} else {
			System.out.println("Game over!");
		}
		sc.close();

	}

	private static void genereerAntwoord() {
		Random random = new Random();
		for (int i = 0; i < 4; i++) {
			antwoord += Integer.toString(random.nextInt(6) + 1);
		}

	}

	public static void print() {
		System.out.println("----");
		for (int lengte = 0; lengte < bord.length; lengte++) {
			System.out.println(bord[lengte]);
		}
	}

	private static void beurt() {
		int[] rij = new int[4];
		String input = Integer.toString(sc.nextInt());

		// toevoegen aan bord
		for (int i = 0; i < rij.length; i++) {
			rij[i] = Character.getNumericValue(input.charAt(i));
		}

		// code invoeren om rij te bepalen

		// rij toevoegen aan het bord
		String s = "";
		for (int lengte = 0; lengte < rij.length; lengte++) {
			s += rij[lengte];
		}
		bord[beurt] = s;
		checkInput(input);
		beurt++;
	}

	private static void checkInput(String input) {
		int correct = 0;
		for (int i = 0; i < antwoord.length(); i++) {
			if (antwoord.charAt(i) == input.charAt(i)) {
				correct++;
			}
		}
		System.out.println("Aantal correcte posities = " + correct);
		if (correct == antwoord.length()) {

			heeftGewonnen = true;
		}
		bord[beurt] += " | " + correct;
	}

	private static void maakBord() {
		for (int i = 0; i < bord.length; i++) {
			bord[i] = "----";
		}
	}

}