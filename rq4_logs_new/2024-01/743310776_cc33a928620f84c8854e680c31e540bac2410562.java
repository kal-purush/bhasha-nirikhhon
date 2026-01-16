package com.vmelnyk.geeks4geeks._20240128LCSOfThreeStrings;

import java.util.*;

class Solution {
  int LCSof3(String a, String b, String c, int m, int n, int o) {
    int[][][] dp = new int[m + 1][n + 1][o + 1];
    for (int i = 1; i <= m; i++) {
      for (int j = 1; j <= n; j++) {
        for (int k = 1; k <= o; k++) {
          if ((a.charAt(i - 1) == b.charAt(j - 1)) && (b.charAt(j - 1) == c.charAt(k - 1))) {
            dp[i][j][k] = 1 + dp[i - 1][j - 1][k - 1];
          } else {
            dp[i][j][k] = Math.max(Math.max(dp[i - 1][j][k], dp[i][j - 1][k]), dp[i][j][k - 1]);
          }
        }
      }
    }
    return dp[m][n][o];
  }
}

class Solution1 {

  record LCSPre(char[] key) {

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LCSPre lcsPre = (LCSPre) o;
      return Arrays.equals(key, lcsPre.key);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(key);
    }
  }

  int LCSof3(String A, String B, String C, int n1, int n2, int n3) {
    // code here

    Map<LCSPre, Set<String>> collector = new HashMap<>();

    extracted(A, collector);
    extracted(B, collector);
    extracted(C, collector);

    char[] longest = new char[0];
    for (Map.Entry<LCSPre, Set<String>> e : collector.entrySet()) {
      final LCSPre key = e.getKey();
      final Set<String> set = e.getValue();
      if (set.size() == 3) {
        if (key.key().length > longest.length) {
          longest = key.key();
        }
      }
    }

    return longest.length;
  }

  private static void extracted(String s, Map<LCSPre, Set<String>> collector) {
    final int length = s.length();
    for (int i = 0; i < length; i++) {
      for (int j = i; j < length; j++) {
        final char[] key = s.substring(i, j + 1).toCharArray();
        Arrays.sort(key);
        collector.computeIfAbsent(new LCSPre(key), k -> new HashSet<>()).add(s);
      }
    }
  }
}