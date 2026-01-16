package com.rizvi.dp.fibonacci.example1;

import java.util.Scanner;

/**
 * Get the nth fibonacci number using Dynamic Programming
 * 
 * @author Rizvi
 *
 */
public class FibonacciDP {
	public static void main(String[] args) {
		Scanner inp = new Scanner(System.in);
		System.out.println("Give the position number: ");
		int positionNumber = inp.nextInt();
		long[] memo = new long[positionNumber + 1];
		System.out.println(positionNumber + "th fibonacci number is: " + fib(positionNumber, memo));
	}

	public static long fib(int n, long[] memo) {
		long result;
		if (memo[n] != 0) {
			System.out.println("memo[" + n + "]: " + memo[n]);
			return memo[n];
		}
		if (n <= 2) {
			result = 1;
		} else {
			result = fib(n - 1, memo) + fib(n - 2, memo);
		}
		memo[n] = result;
		return result;
	}
}