package week_02.assignments;

import java.util.Scanner;

public class Question_02_06 {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

        // Prompt the user to enter a number between 0 and 1000.
        System.out.print("Enter a number between 0 and 1000: ");
        int number = input.nextInt();

        // Compute the sum of the digits in the integer.

        // Extract the digit less than 10
        int lessThan10 = number % 10;
        // Remove the extracted digit
        number /= 10;
        // Extract the digit between 10 to 99
        int tens = number % 10;
        // Remove the extracted digit
        number /= 10;
        // Extract the digit between 100 to 999
        int hundreds = number % 10;
        // Remove the extracted digit
        number /= 10;

        int sum = hundreds + tens + lessThan10;

        // Display results
        System.out.println("The sum of the digits is " + sum);
    }
}