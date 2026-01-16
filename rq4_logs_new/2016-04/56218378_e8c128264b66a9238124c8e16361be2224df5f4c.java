package oblig4;

import java.util.Scanner;

public class SnuTekst {
	
	public static void main(String[] args){
		Scanner input = new Scanner(System.in);
		
		System.out.print("Vennlig skriv en streng: ");
		String s = input.next();
		System.out.print("Baklengs streng: ");	
		baklengs(s);
		
		input.close();
	}
	
	public static void baklengs(String tekst){

		if (tekst.length() <= 1)
			System.out.print(tekst);
		else{
			System.out.print(tekst.substring(tekst.length()-1));
			baklengs(tekst.substring(0, tekst.length()-1));			
		}
	}
}