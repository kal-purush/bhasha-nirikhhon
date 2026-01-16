import java.util.Random;
import java.util.Scanner;

public class GuessGame{
	public static void main(String[] args){
		Random rand = new Random();
		int numberToGuess = rand.nextInt(100);
		int numberOfTries = 0;
		Scanner input = new Scanner(System.in);
		int guess;
		boolean win = false;

		System.out.println("I am thinking of a number from 1 to 100 ... guess what it is?");       

		while(win == false){

			guess = input.nextInt();
			numberOfTries++;

			if(guess < numberToGuess){
				System.out.println("Your guess is lower!");
				System.out.println("Try Again!");
			}
			else if(guess > numberToGuess){
				System.out.println("Your guess is Higher!");
				System.out.println("Try Again!");
			}    
			else if(guess == numberToGuess){
				win = true;
				System.out.println("Congratulations. You guessed the number in " + numberOfTries + " tries!");
			}
		}
	}
}