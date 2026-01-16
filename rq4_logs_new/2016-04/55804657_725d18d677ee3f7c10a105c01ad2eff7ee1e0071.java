package test;

import java.util.Random;
import java.util.Scanner;

/*
 * basically just an example of how we would mess with probability of shit
 */

public class Probability {
	
	public static void main(String[] args) {
		
		int goToPancheros, dontGoToPancheros;
		Random maybe = new Random();
		boolean didWeGoYesterday = false;
		boolean go;
		
		goToPancheros = 99;
		dontGoToPancheros = 1;
		
		Scanner sc = new Scanner(System.in);
		
		System.out.println("how many days?");
		int days = sc.nextInt();
		
		for (int i = 0; i < days; i++) {
			if (!didWeGoYesterday) {
				goToPancheros = 99;
				dontGoToPancheros = 1;
				if (maybe.nextInt(100) <= dontGoToPancheros) {
					go = false;
				} else {
					go = true;
					didWeGoYesterday = true;
				}
			} else {
				goToPancheros--;
				dontGoToPancheros++;
				if (maybe.nextInt(100) <= dontGoToPancheros) {
					go = false;
				} else {
					go = true;
					didWeGoYesterday = true;
				}
			}
		}
		
		
		sc.close();
	}

}