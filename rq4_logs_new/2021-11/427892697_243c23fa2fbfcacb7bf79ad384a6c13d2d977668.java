package HW;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        // write your code here
        // Follow these steps:
        // Create a new file called factorial.java
        // Write a program that determines the factorial of a number entered by a user.
        // Prompt the user to enter a positive integer.
        Scanner input = new Scanner(System.in);
        System.out.println("Enter positive number:");
        int num = input.nextInt();
        while (num<0){
            System.out.println("the number most be positive!\n Enter positive number:");
            num = input.nextInt();
        }
        // Then calculate the factorial of the given number. For any positive integer n, its factorial is given by:
        // factorial = 123...*n
        int factorial = num;
        int calc = 1;
        while (factorial!=0){
            calc *= factorial;
            factorial= factorial-1;
        }
        // Finally, print out the factorial
        System.out.println(calc);
    }
}