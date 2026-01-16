package learn.ijpds2nd.ch10oop;

import learn.ijpds2nd.tools.ConsoleInput;

import java.math.BigInteger;

/* Listing 10.9 */
public class LargeFactorial {
    public static void main(String[] args) {
        ConsoleInput in = new ConsoleInput();
        int n = in.requestInt("Enter an integer: ");
        BigInteger result = factorial(n);
        System.out.printf("%d! is %s%n", n, result);
    }

    private static BigInteger factorial(int n) {
        BigInteger result = BigInteger.ONE;
        for (int i = 2; i <= n; i++) {
            result = result.multiply(BigInteger.valueOf(i));
        }
        return result;
    }
}