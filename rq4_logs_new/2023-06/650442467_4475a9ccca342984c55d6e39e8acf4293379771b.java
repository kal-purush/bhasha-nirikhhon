package day4_functions;

import java.util.Scanner;

public class P13_PrimeNumbersBetweenTwoNumber {
    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        System.out.println("Enter two number");
        int n1 = in.nextInt();
        int n2 = in.nextInt();
        System.out.println("Prime Numebers Between This two are :");
        for (int i = n1; i <= n2; i++) {
            if (isPrime(i)) {
                System.out.println(i);
            }
        }
        in.close();
    }

    static boolean isPrime(int num) {
        for (int i =2; i < num; i++) {
            if (num % i == 0)
                return false;
        }
        return true;
    }
}