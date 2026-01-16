package org.example.exhaustive_search;

import java.io.*;
import java.util.StringTokenizer;

public class Back13702 {
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  /*
2 3
702
429

   */
  static int N, K;
  static int[] A;

  static void input() {
    N = scan.nextInt();
    K = scan.nextInt();
    A = new int[N + 1];
    for (int i = 1; i <= N; i++) {
      A[i] = scan.nextInt();
    }
  }

  static boolean determination(long W) {
    int cnt = 0;
    for (int i = 1; i <= N; i++) {
      if (A[i] < W) continue;
      if (W < 1) continue;
      else {
        cnt += (A[i] / W);
      }
    }

    return cnt >= K;
  }

  static void pro() {
    long L = 0, R = Integer.MAX_VALUE, ans = 0;

    while (L <= R) {
      long mid = (L + R) / 2;
      if (determination(mid)) {
        L = mid + 1;
        ans = mid;
      } else {
        R = mid - 1;
      }
    }

    System.out.println(ans);
  }

  public static void main(String[] args) {
    input();
    pro();
  }


  static class FastReader {
    BufferedReader br;
    StringTokenizer st;

    public FastReader() {
      br = new BufferedReader(new InputStreamReader(System.in));
    }

    public FastReader(String s) throws FileNotFoundException {
      br = new BufferedReader(new FileReader(new File(s)));
    }

    String next() {
      while (st == null || !st.hasMoreElements()) {
        try {
          st = new StringTokenizer(br.readLine());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return st.nextToken();
    }

    int nextInt() {
      return Integer.parseInt(next());
    }

    long nextLong() {
      return Long.parseLong(next());
    }

    double nextDouble() {
      return Double.parseDouble(next());
    }

    String nextLine() {
      String str = "";
      try {
        str = br.readLine();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return str;
    }
  }
}