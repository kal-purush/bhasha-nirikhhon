import java.util.InputMismatchException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        boolean playAgain;

        do {
            // Set the range for the guessing game
            int min = 0;
            int max = 100;
            int myNumber = (int) (Math.random() * (max - min)) + min; // Generate random number
            int userNumber = 0;
            int maxGuesses = 10; // Maximum number of guesses allowed
            int guessCount = 0;

            System.out.println("Welcome to the Number Guessing Game!");
            System.out.println("I have selected a number between " + min + " and " + max + ". Try to guess it!");
            System.out.println("You have " + maxGuesses + " attempts to guess the number.");

            do {
                System.out.print("Guess my number: ");
                try {
                    userNumber = sc.nextInt(); // Read user input
                    guessCount++; // Increment guess count

                    // Check if the guess is within range
                    if (userNumber < min || userNumber > max) {
                        System.out.println("Please guess a number within the range of " + min + " to " + max + ".");
                        continue; // Skip the rest of the loop
                    }

                    if (userNumber == myNumber) {
                        System.out.println("Woohoo! Your number is correct!");
                        break; // Exit the loop if the guess is correct
                    } else if (userNumber > myNumber) {
                        System.out.println("Your number is too large. Try again.");
                    } else {
                        System.out.println("Your number is too small. Try again.");
                    }

                    // Check if the user has exceeded the maximum number of guesses
                    if (guessCount >= maxGuesses) {
                        System.out.println("Sorry, you've used all your guesses! The correct number was: " + myNumber);
                        break; // Exit the loop if max guesses are reached
                    }

                } catch (InputMismatchException e) {
                    System.out.println("Please enter a valid integer.");
                    sc.next(); // Clear the invalid input
                }
            } while (true);

            // Ask the user if they want to play again
            System.out.print("Do you want to play again? (yes/no): ");
            String response = sc.next();
            playAgain = response.equalsIgnoreCase("yes");

        } while (playAgain);

        System.out.println("Thanks for playing! Goodbye.");
        sc.close(); // Close the scanner
    }
}