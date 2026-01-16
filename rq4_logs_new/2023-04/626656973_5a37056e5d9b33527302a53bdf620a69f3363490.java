import java.util.Scanner;

public class T10 {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        // ask user for input values of a and b
        int a = input.nextInt();
        int b = input.nextInt();
        // compute the GCD using recursion
        int gcd = findGCD(a, b);
        // output gcd value
        System.out.println(gcd);
    }
    // recursive function to compute the GCD of two numbers
    public static int findGCD(int a, int b) {
        // if b is 0, the GCD is a
        if (b == 0) {
            return a;
        }
        else if (a == 0){
            return b;
        }
        // compute the GCD using the Euclidean algorithm
        return findGCD(b, a % b);
    }
}