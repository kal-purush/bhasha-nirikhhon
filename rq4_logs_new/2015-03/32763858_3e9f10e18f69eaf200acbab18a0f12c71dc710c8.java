package org.shyam.test;

import java.util.Scanner;

/**
 * @author Shyam
 * 
 *We are using Binary Search Algorithm for this number Guess game
 *and Assuming the user would guess between 1 and 100
 *
 */

public class NumberGuess {
	

	public static void main(String[] args) {

		numbergame();

	}

	public  static void numbergame() {
		Scanner userInput = null;
		try {
			int first, last, middle;
			userInput = new Scanner(System.in);
			first = 1;
			last = 100;
			middle = (first + last) / 2;
			
			System.out.println("please guess numbers between 1 to 100 and Enter 'READY ' to start the game ");

			String response = "";
			
			/*
			 * Game start validation start
			 */
			
			while (userInput.hasNext()) {
				response = userInput.next();
				if (response.equalsIgnoreCase("READY")) {
					System.out.println("is the number ? " + middle);
					break;
				} else {
					System.out.println("please enter ready to start game");
					continue;
				}
			}
			
			/*
			 * Game start validation End
			 */
			
			/*
			 * Guess number logic 
			 */
			
			while (userInput.hasNext()) {

				response=userInput.next();
				if (response.equalsIgnoreCase("YES")) {
					System.out.println("Hurray !!! we found your number ");
					break;

				} else if (response.equalsIgnoreCase("END")) {
					System.out.println("we are sorry to see u go in the middle of game");
					break;

				} else if (response.equalsIgnoreCase("Higher")) {

					first = middle;
					middle = (last + first) / 2;

				} else if (response.equalsIgnoreCase("lower")) {
					last = middle;
					middle = (last + first) / 2;
				} else {
					System.out.println("please enter Either(HIGHER,LOWER,YES,END)");
					continue;
				}

				System.out.println("is the number? " + middle);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("error"+e.getMessage());
		} finally {
			userInput.close();
		}

	}
}