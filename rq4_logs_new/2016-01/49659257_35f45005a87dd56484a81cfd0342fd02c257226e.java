package zadaci_29_01_2016;

import java.util.Scanner;

public class Matrica {

	public static void printMatrix(int n) {
		// dupla for petlja za ispis redova i kolona matrice
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < n; j++) {
				// ispisujemo broj 1 ili 0 koji dobijemo uz pomoc math.random
				// metode
				System.out.print((int) (Math.random() * 2) + " ");
			} // prelazak u novi red
			System.out.println();
		}
	}

	public static void main(String[] args) {
		// Napisati metodu koja ispisuje n x n matricu koristeći se sljedećim
		// headerom: public static void printMatrix(int n). Svaki element u
		// matrici je ili 0 ili 1, nasumično generisana. Napisati test program
		// koji pita korisnika da unese broj n te mu ispiše n x n matricu u
		// konzoli.

		// skener za unos
		Scanner input = new Scanner(System.in);
		// ispis
		System.out.println("Unesite broj n za ispisati n x n matricu");
		// smjesamo n i proljedjuemo ga u drugom redu metodi
		int n = input.nextInt();
		// poziv metode
		printMatrix(n);
		// zatvarmo input
		input.close();
	}
}