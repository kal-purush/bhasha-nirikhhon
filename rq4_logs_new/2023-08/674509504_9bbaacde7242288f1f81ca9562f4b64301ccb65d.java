package week_02.assignments;

import java.util.Scanner;

public class Question_02_11 {
    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);

        // Prompt the user to the number of years
        System.out.println("Enter the number of years : ");
        int years = input.nextInt();

        // Calculate population projection
        int population= (int) (312032486 + (years * ((60 * 60 * 24 * 365) / 7.0) - (60 * 60 * 24 * 365) / 13.0) + ((60 * 60 * 24 * 365) / 45.0));

        // Display the results
        System.out.println("The population in " + years + " years is " + population);
    }
}