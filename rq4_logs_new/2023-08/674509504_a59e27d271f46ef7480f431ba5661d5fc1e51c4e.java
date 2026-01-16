package week_02.assignments;

import java.util.Scanner;

public class Question_02_07 {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

        // Prompt the user to enter the number of minutes
        System.out.print("Enter the number of minutes: ");
        int minutes = input.nextInt();

        // Compute the number of years and days
        int years = minutes / 525600;
        int days = (minutes % 525600) / 1440;

        // Display results
        System.out.println(minutes + " minutes is approximately " + years
                + " years and " + days + " days");
    }
}