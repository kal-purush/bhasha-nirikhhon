//https://leetcode.com/problems/fibonacci-number/description/
package algorithms.easy;

public class FibonacciNumber {
    public static void main(String[] args) {
        FibonacciNumber fibonacci = new FibonacciNumber();
        System.out.println("Output:\t" + fibonacci.fib(2));
        System.out.println("Output:\t" + fibonacci.fib(3));
        System.out.println("Output:\t" + fibonacci.fib(4));
    }

    public int fib(int n) {
        int[] dp = new int[n + 1];
        dp[0] = 0;
        if (n > 0) dp[1] = 1;
        int i = 2;
        while (n >= i) {
            dp[i] = dp[i - 1] + dp[i - 2];
            i++;
        }
        return dp[n];
    }
}