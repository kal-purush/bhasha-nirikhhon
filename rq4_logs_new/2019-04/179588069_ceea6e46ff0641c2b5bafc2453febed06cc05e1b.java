package Task1;

import java.util.InputMismatchException;
import java.util.Scanner;

public class PrimesFinder {
    public static void main(String[] args) {
        try {
            Scanner in = new Scanner(System.in);
            System.out.println("Enter start: ");
            long start = in.nextLong();
            System.out.println("Enter end: ");
            long end = in.nextLong();
            findPrimes(start, end);
        } catch (InputMismatchException ex){
            System.out.println("You entered incorrect values");
        }
    }

    public static void findPrimes(long a, long b){
        int count = 0;
        for(long i = a; i <= b; i++){
            if(isPrime(i)){
                count++;
                System.out.print(i + " ");
                if(count == a){
                    break;
                }
            }
        }
    }

    public static boolean isPrime(long n){
        if(n == 0 || n == 1){
            return false;
        }
        for(int i = 2; i <= n/2; i++){
            if(n % i == 0){
                return false;
            }
        }
        return true;
    }
}