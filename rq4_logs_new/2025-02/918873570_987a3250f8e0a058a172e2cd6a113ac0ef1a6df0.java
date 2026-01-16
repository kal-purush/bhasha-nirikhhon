//Write a Program to enter principal, time and scheme choice -
// Annual interest payout or Cumulative interest payout.
// Annual interest payout means interest would be paid at the end of the year of investment and
// principal amount remains same for the next year.
// Interest is calculated using simple interest method.
// Cumulative interest payout means interest would be paid at the end of the period for investment and
// principal amount changes by adding the interest earned till last year.
// Interest is calculated using compound interest method.
// For investment up to 1 year rate is 6%, up to 2 years it is 7% and above that it is 8% per annuam.
// The program should print the interest amount that the person will earn according to the two schemes.
package com.learn.conditional_statement;

import java.util.Scanner;

public class Q20 {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Display scheme options
        System.out.println("------------------ Scheme ------------------");
        System.out.println("1. Annual Interest Payout (Simple Interest)");
        System.out.println("2. Cumulative Interest Payout (Compound Interest)");
        System.out.println("--------------------------------------------");
        System.out.print("Enter your choice (1 or 2): ");
        int scheme = scanner.nextInt();

        // Validate scheme choice
        if (scheme != 1 && scheme != 2) {
            System.out.println("Invalid choice. Exiting program.");
            return;
        }

        // Input principal and time
        System.out.print("Enter Principal Amount: ");
        double principal = scanner.nextDouble();
        System.out.print("Enter Time Period (in years): ");
        int time = scanner.nextInt();

        // Validate time period
        if (time < 1) {
            System.out.println("Invalid Time Period. Exiting program.");
            return;
        }

        // Determine interest rate based on time
        double rate = getInterestRate(time);

        // Calculate and display interest based on the chosen scheme
        if (scheme == 1) {
            double simpleInterest = calculateSimpleInterest(principal, rate, time);
            System.out.printf("Interest earned with Annual Interest Payout: %.2f%n", simpleInterest);
        } else {
            double compoundInterest = calculateCompoundInterest(principal, rate, time);
            System.out.printf("Interest earned with Cumulative Interest Payout: %.2f%n", compoundInterest);
        }
    }

    // Method to determine the interest rate based on time
    private static double getInterestRate(int time) {
        if (time == 1) {
            return 6;
        } else if (time == 2) {
            return 7;
        } else {
            return 8;
        }
    }

    // Method to calculate simple interest
    private static double calculateSimpleInterest(double principal, double rate, int time) {
        return (principal * rate * time) / 100;
    }

    // Method to calculate compound interest
    private static double calculateCompoundInterest(double principal, double rate, int time) {
        return principal * Math.pow(1 + (rate / 100), time) - principal;
    }
}