package fr.eni.hello;

import java.util.Scanner;

public class Bonjour {

	public static void main(String[] args) {
		
		Scanner scan = new Scanner(System.in);
		
		System.out.println("Veuillez svp saisir un nombre");
		
		int nombre = scan.nextInt();
		
		int modulo = nombre%2;
		
		if(modulo == 0) {
			System.out.println("Ce nombre est pair");			
		} else {
			System.out.println("Ce nombre est impair");
		}
				
	}

}