package br.dev.ferreiras.challenges.leetCode;

import javax.management.Query;

/*
Given two integers dividend and divisor, divide two integers without using multiplication,
division, and mod operator.

The integer division should truncate toward zero, which means losing its fractional part.
For example, 8.345 would be truncated to 8, and -2.7335 would be truncated to -2.

Return the quotient after dividing dividend by divisor.

Note:
Assume we are dealing with an environment that could only store integers within the 32-bit
signed integer range: [−2exp31, 2exp31 − 1].
For this problem, if the quotient is strictly greater than 2exp31 - 1,
then return 2exp31 - 1, and if the quotient is strictly less than -2exp31,
then return -2exp31.



Example 1:

Input: dividend = 10, divisor = 3
Output: 3
Explanation: 10/3 = 3.33333.. which is truncated to 3.
Example 2:

Input: dividend = 7, divisor = -3
Output: -2
Explanation: 7/-3 = -2.33333.. which is truncated to -2.


Constraints:

-231 <= dividend, divisor <= 231 - 1
divisor != 0
 */
public class DivideTwoIntegers {
  public static void main(String[] args) {
    int i = 20, j = 5;

    int r = divide(i,j);
    System.out.println(r);
  }
  public static int divide(int dividend, int divisor) {
    int left = 0, middle = 0, right = 0, quotient = 0;
    right = dividend;
    while (left <= right) {

      middle = left + (right - left) / 2;

      if ((middle * divisor) <=  dividend) {
        quotient = middle;
        left = middle + 1;
      } else {
        right = middle - 1;
      }
    }
    return ((dividend < 0 && divisor < 0) || (dividend >= 0 && divisor >= 0) ? quotient : -quotient);
  }
}