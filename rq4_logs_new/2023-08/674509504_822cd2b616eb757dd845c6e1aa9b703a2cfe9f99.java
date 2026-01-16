package week_02.assignments;
import java.util.Scanner;

public class Question_02_10 {
    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);

        // Prompt the user to enter the amount of water in kilograms.
        System.out.print("Enter the amount of water in kilograms: ");
        double kilograms = input.nextDouble();
        // Prompt the user to enter the initial temperature.
        System.out.print("Enter the initial temperature: ");
        double initialTemperature = input.nextDouble();
        // Prompt the user to enter the final temperature.
        System.out.print("Enter the final temperature: ");
        double finalTemperature = input.nextDouble();

        // Calculate the energy
        double energy = kilograms * (finalTemperature - initialTemperature) * 4184;

        // Display result
        System.out.println("The energy needed is " + energy);
    }
}