package week_02.assignments;

import java.util.Scanner;

public class Question_02_03 {
    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);

        // Create constant value
        final double METERS_PER_FOOT = 0.305;

        // Prompt user to enter a number in feet
        System.out.print("Enter a value for feet: ");
        double feet = input.nextDouble();

        // Convert feet into meters
        double meters = feet * METERS_PER_FOOT;

        // Display results
        System.out.println(feet + " feet is " + meters + " meters");
    }
}