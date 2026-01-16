// Написать программу вычисления n-ого треугольного числа.

import java.util.Scanner;

public class HW11 {
    /** Function returns an int number that was inputed from the console
    */
    static int Output() {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Input a number: ");
        int number  = scanner.nextInt();
        System.out.print("\n");
        scanner.close();
        return number;
    }
    /** Function calculates a triangle number
    * @param number the number of the triangular number
    */  
    static float Triangle(int number) {
        float result = 0;
        for (int i = 1; i <= number; i++) {
            result = (float)(i * (i + 1)) / 2;
        }
        return result;
    }
    public static void main(String[] args) {
        int number = Output();
        float result = Triangle(number);
        System.out.println(String.format("%-5s= %s", "Triangular number of " + number + " ", result));
    }
}