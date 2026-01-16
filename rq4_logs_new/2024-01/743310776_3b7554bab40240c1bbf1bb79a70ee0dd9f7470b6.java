package com.vmelnyk.leetcode;

// Fibonacci Series using Optimized Method
/*
https://www.geeksforgeeks.org/program-for-nth-fibonacci-number/?ref=lbp

Nth Power of Matrix Approach to Find and Print Nth Fibonacci Numbers
This approach relies on the fact that if we n times multiply the matrix
M = {{1,1},{1,0}} to itself (in other words calculate power(M, n)),
then we get the (n+1)th Fibonacci number as the element at row and column (0, 0) in the resultant matrix.

If n is even then k = n/2:
Therefore Nth Fibonacci Number = F(n) = [2*F(k-1) + F(k)]*F(k)
If n is odd then k = (n + 1)/2:
Therefore Nth Fibonacci Number = F(n) = F(k)*F(k) + F(k-1)*F(k-1)

Taking determinant on both sides, we get (-1)n = Fn+1Fn-1 – Fn2

Moreover, since AnAm = An+m for any square matrix A, the following identities can be
derived (they are obtained from two different coefficients of the matrix product)

FmFn + Fm-1Fn-1 = Fm+n-1         —————————(1)

By putting n = n+1 in equation(1),

FmFn+1 + Fm-1Fn = Fm+n             ————————–(2)

Putting m = n in equation(1).

F2n-1 = Fn2 + Fn-12


Putting m = n in equation(2)

F2n = (Fn-1 + Fn+1)Fn = (2Fn-1 + Fn)Fn  ——–

( By putting Fn+1 = Fn + Fn-1 )

To get the formula to be proved, we simply need to do the following

If n is even, we can put k = n/2
If n is odd, we can put k = (n+1)/2

 */
public final class Fibonacci {
  /* function that returns nth Fibonacci number */
  static int fib(int n) {
    final int[][] F = new int[][] {{1, 1}, {1, 0}};
    if (n == 0) {
      return 0;
    }

    power(F, n - 1);

    return F[0][0];
  }

  static void multiply(int[][] F, int[][] M) {
    final int x = F[0][0] * M[0][0] + F[0][1] * M[1][0];
    final int y = F[0][0] * M[0][1] + F[0][1] * M[1][1];
    final int z = F[1][0] * M[0][0] + F[1][1] * M[1][0];
    final int w = F[1][0] * M[0][1] + F[1][1] * M[1][1];

    F[0][0] = x;
    F[0][1] = y;
    F[1][0] = z;
    F[1][1] = w;
  }

  /* Optimized version of power() in method 4 */
  static void power(int[][] F, int n) {
    if (n == 0 || n == 1) {
      return;
    }
    final int[][] M = new int[][] {{1, 1}, {1, 0}};

    power(F, n / 2);
    multiply(F, F);

    if (n % 2 != 0) {
      multiply(F, M);
    }
  }

  /* Driver program to test above function */
  public static void main(String[] args) {
    int n = 9;
    System.out.println(fib(n));
  }
}