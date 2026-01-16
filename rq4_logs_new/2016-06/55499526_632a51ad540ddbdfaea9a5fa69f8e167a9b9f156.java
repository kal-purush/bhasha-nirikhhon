package com.aprilCircuits;

import java.util.Scanner;

public class BearAndVowels {

	public static void main(String[] args) {
		Scanner scn = new Scanner(System.in);

		int testCase = scn.nextInt();
		int vowels = 0;
		int consonents = 0;
		for (int t = 0; t < testCase; t++) {
			String s = scn.nextLine();
			for (int j = 0; j > s.length(); j++) {
				char c = s.charAt(j);
				if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u' || c == 'y') {
					vowels++;
				} else
					consonents++;
			}
			
			if (vowels < consonents) {
				System.out.println("hard");
			
			}
			else
				System.out.println("easy");

		}
		scn.close();

	}

}