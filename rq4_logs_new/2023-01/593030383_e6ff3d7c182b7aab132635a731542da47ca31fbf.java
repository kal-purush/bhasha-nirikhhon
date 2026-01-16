package com.java.atm;

import java.util.Scanner;

public class ATM {
    static double balance = 1000.0;
    static Scanner sc = new Scanner(System.in);

    public static void main(String[] args) {
        int choice;

        do {
            System.out.println("Welcome to the ATM");
            System.out.println("1. Check Balance");
            System.out.println("2. Withdraw");
            System.out.println("3. Deposit");
            System.out.println("4. Exit");
            System.out.print("Enter your choice: ");
            choice = sc.nextInt();

            switch (choice) {
                case 1:
                    checkBalance();
                    break;
                case 2:
                    withdraw();
                    break;
                case 3:
                    deposit();
                    break;
                case 4:
                    System.out.println("Thank you for using our ATM. Have a nice day!");
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        } while (choice != 4);
    }

    public static void checkBalance() {
        System.out.println("Your current balance is: $" + balance);
    }

    public static void withdraw() {
        double amount;
        System.out.print("Enter amount to withdraw: $");
        amount = sc.nextDouble();

        if (amount > balance) {
            System.out.println("Insufficient balance.");
        } else {
            balance = balance - amount;
            System.out.println("Please collect your cash.");
            System.out.println("Your current balance is: $" + balance);
        }
    }

    public static void deposit() {
        double amount;
        System.out.print("Enter amount to deposit: $");
        amount = sc.nextDouble();
        balance = balance + amount;
        System.out.println("Your current balance is: $" + balance);
    }
}