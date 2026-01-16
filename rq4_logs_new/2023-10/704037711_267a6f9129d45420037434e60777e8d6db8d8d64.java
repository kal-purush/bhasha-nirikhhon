package O1_java_basics.O13_22042022_beginners_maths;

import java.util.Scanner;

public class SumOfDivisors {
    private static int getSumOfDivisors (int num){
        int sum = 0;

        for( int i = 1; i * i <= num; i++ ) {
            if(num % i == 0) {
                if(i*i == num || i == 1) { // This condition checks if i * i == num (To handle perfect squares)
                    sum = sum + i;
                }
                else {
                    sum = sum + i + (num/i);
                }
            }
        }

        return sum;
    }

    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter a number to fetch the sum of divisors for");
        int num = sc.nextInt();
        System.out.println(getSumOfDivisors(num));
    }
}