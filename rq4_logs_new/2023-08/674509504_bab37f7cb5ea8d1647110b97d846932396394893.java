package week_02.assignments;

import java.util.Scanner;

public class Question_02_04 {
    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);

        // Create constant value
        final double KILOGRAMS_PER_POUND = 0.454;

        // Prompt user to enter the number of pounds
        System.out.print("Enter a number in pounds: ");
        double pounds = input.nextDouble();

        // Convert pounds into kilograms
        double kilograms = pounds * KILOGRAMS_PER_POUND;

        // Display the results
        System.out.println(pounds + " pounds is " + kilograms + " kilograms");
    }
}