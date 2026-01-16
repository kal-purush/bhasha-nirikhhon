package com.vmelnyk.geeks4geeks._20240128CountDigitGroupingsOfNmber;

import java.util.Arrays;

class Solution {
  public int TotalCount(String str) {
    // Code here
    int i, j, result = 1, n = str.length();
    int[][] sums = new int[n][n];

    for (i = 0; i < n; i++) {
      for (j = i; j < n; j++) {
        final int v = str.charAt(j) - '0';
        if (i == j) {
          sums[i][j] = v;
        } else {
          sums[i][j] = sums[i][j - 1] + v;
        }
      }
    }

    int[][] dp = new int[n][n];
    Arrays.fill(dp[0], 1);

    for (i = 1; i < n; i++) {
      for (j = i; j < n; j++) {
        for (int k = 0; k <= i - 1; k++) {
          if (sums[k][i - 1] <= sums[i][j]) {
            dp[i][j] += dp[k][i - 1];
          }
        }
      }
      result += dp[i][n - 1];
    }

    return result;
  }
}