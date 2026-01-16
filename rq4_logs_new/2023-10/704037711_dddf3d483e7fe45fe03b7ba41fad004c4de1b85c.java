package O1_java_basics.O13_22042022_beginners_maths;

import java.util.Scanner;

public class Prime {
    private static boolean isPrime(int num) {
        // if i < root of num
        for(int i = 2; i * i <= num; i++) {
            if(num % i == 0) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int num = sc.nextInt();
        if(isPrime(num)) {
            System.out.print(num + " is a prime number.");
        }
        else {
            System.out.print(num + " is not a prime number.");
        }
    }
}