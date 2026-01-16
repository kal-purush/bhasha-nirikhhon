package edu.brown.cs.termproject.trie;

/**
 * This class calculates led.
 *
 * @author sy69
 */

public final class LDistanceCalculator {

  private LDistanceCalculator() {
  }


  private static int minimum(int a, int b, int c) {
    int mi;

    mi = a;
    if (b < mi) {
      mi = b;
    }
    if (c < mi) {
      mi = c;
    }
    return mi;
  }

  /**
   * This is the calculate method.
   *
   * @param s sting1
   * @param t string2
   * @return led distance
   */

  public static int calculate(String s, String t) {
    int[][] d; // matrix
    int n; // length of s
    int m; // length of t
    int i; // iterates through s
    int j; // iterates through t
    char si; // ith character of s
    char tj; // jth character of t
    int cost;

    n = s.length();
    m = t.length();
    if (n == 0) {
      return m;
    }
    if (m == 0) {
      return n;
    }
    int a = n + 1;
    int b = m + 1;
    d = new int[a][b];
    for (i = 0; i <= n; i++) {
      d[i][0] = i;
    }

    for (j = 0; j <= m; j++) {
      d[0][j] = j;
    }

    for (i = 1; i <= n; i++) {

      si = s.charAt(i - 1);

      for (j = 1; j <= m; j++) {

        tj = t.charAt(j - 1);

        if (si == tj) {
          cost = 0;
        } else {
          cost = 1;
        }

        d[i][j] = minimum(d[i - 1][j] + 1,
            d[i][j - 1] + 1, d[i - 1][j - 1] + cost);

      }

    }

    return d[n][m];

  }


}