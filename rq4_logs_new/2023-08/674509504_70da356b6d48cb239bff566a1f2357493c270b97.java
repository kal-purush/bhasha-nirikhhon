package week_02.assignments;

import java.util.Scanner;

public class Question_02_12 {
    public static void main(String[] args) {

        Scanner input = new Scanner(System.in);

        // Prompt user to enter the airplane's acceleration a and take-off speed v.
        System.out.print("Enter speed and acceleration: ");
        double speed = input.nextDouble();
        double acceleration = input.nextDouble();

        // Compute the minimum runway length
        // needed for an airplane to take off.
        double length = Math.pow(speed, 2) / (2 * acceleration);

        // Display result
        System.out.println("The minimum runway length for this" +
                " airplane is " + length);
    }
}