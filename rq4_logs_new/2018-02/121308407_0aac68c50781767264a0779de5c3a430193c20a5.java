
/*
    Nguyen, Nguyen
    CS A170
    02/11/2018

    IC03_ChangeMaker
 */
import java.util.Scanner;

public class ChangeMaker {

	public static void main(String[] args) {
		// Variables
		int cents, dimes, quarters, nickels, pennies;
		Scanner scanner = new Scanner(System.in);

		// Read input
		cents = Prompt(
				"Enter a whole number from 1 to 99\nI will find a combination of coins to equal that amount of change.\n",
				scanner);

		// Close the scanner
		scanner.close();

		// Calculation
		quarters = cents / 25;
		cents %= 25; // The remain

		dimes = cents / 10;
		cents %= 10; // The remain

		nickels = cents / 5;
		cents %= 5; // The remain

		pennies = cents;

		// Display the results
		System.out.println(quarters + " quarter(s)");
		System.out.println(dimes + " dime(s)");
		System.out.println(nickels + " nickel(s)");
		System.out.println(pennies + " penny(ies)");

	}

	public static int Prompt(String message, Scanner scanner) {
		// Variables
		int result;

		// Print the message
		System.out.print(message);

		// Loop until user enters a valid number
		while (!scanner.hasNextDouble()) {
			// Print error
			System.out.println("\nError. You must to enter a valid number.");

			// Print the message again
			System.out.print(message);

			// Next token
			scanner.next();
		}

		// Get the number
		result = scanner.nextInt();

		// Check if the number is positive or negative
		if (result >= 0) {
			// Return the number
			return result;
		} else {
			// Print error
			System.out.println("\nSorry. You should enter a POSITIVE number.\n");

			// Recursion
			return Prompt(message, scanner);
		}
	}
}