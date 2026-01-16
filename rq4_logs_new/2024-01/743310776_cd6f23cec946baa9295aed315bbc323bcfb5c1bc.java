package com.vmelnyk.geeks4geeks._20240127GeekinaHate1s;

import java.util.Arrays;

class Solution {

  private final Long[][][] dp = new Long[2][65][65];

  public long findNthNumer(int n, int k) {
    // Code Here.
    long low = 0, high = (long) (1e18);
    while (low <= high) {
      long mid = low + (high - low) / 2;
      long count = find(mid, k);
      if (count >= n) {
        high = mid - 1;
      } else {
        low = mid + 1;
      }
    }
    return low;
  }

  private long find(long n, int k) {
    char[] s = Long.toBinaryString(n).toCharArray();
    reset();
    return dp(s, s.length, 1, k);
  }

  private long dp(char[] s, int n, int tight, int k) {
    if (k < 0) {
      return 0;
    }
    if (n == 0) {
      return 1;
    }
    if (dp[tight][k][n] != null) {
      return dp[tight][k][n];
    }
    int upperBit = (tight == 1 ? (s[s.length - n] - '0') : 1);
    long result = 0;
    for (int bit = 0; bit <= upperBit; bit++) {
      if (bit == upperBit) {
        result += dp(s, n - 1, tight, k - bit);
      } else {
        result += dp(s, n - 1, 0, k - bit);
      }
    }
    return dp[tight][k][n] = result;
  }

  private void reset() {
    for (int i = 0; i < 65; i++) {
      Arrays.fill(dp[0][i], null);
      Arrays.fill(dp[1][i], null);
    }
  }
}

class Solution2 {
  public long nCr(long n, long r) {
    if (n < r) {
      return 0;
    }

    long res = 1;
    for (int i = 1; i <= r; ++i) {
      res = res * (n - r + i) / i;
    }

    return res;
  }

  public long reachLast(int bit, int k) {
    long res = 1;

    for (int i = 1; i < k; ++i) {
      res += nCr(bit, i);
    }

    return res;
  }

  public long rep(long n, int k, int bit) {
    long res = 1L << bit;

    if (n == 1) {
      return res;
    }

    for (int cur = bit - 1; cur > 0; --cur) {

      long nextOnes = 1L;
      for (int i = cur - 1; i >= 0; --i) {
        nextOnes += reachLast(i, k);
      }

      if (nextOnes >= n) {
        continue;
      }

      --k;
      res |= (1L << cur);
      n -= nextOnes;

      if (n == 1) {
        return res;
      }
    }

    if (n == 2) {
      res |= 1L;
    }

    return res;
  }

  public long findNthNumer(int n, int k) {
    if (--n == 0) {
      return 0;
    }

    for (int i = 0; i <= 63; ++i) {

      long result = reachLast(i, k);
      if (n <= result) {
        return rep(n, k - 1, i);
      }

      n -= result;
    }

    return 0;
  }
}