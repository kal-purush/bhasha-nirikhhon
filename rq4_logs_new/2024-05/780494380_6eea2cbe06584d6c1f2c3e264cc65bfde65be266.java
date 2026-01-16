package org.example.exhaustive_search;

import java.io.*;
import java.util.StringTokenizer;

public class back6236 {
  /*
7 5
100
400
300
100
500
101
400

7 5
100
200
300
400
500
600
700

   */

  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N, M;
  static int A[];

  private static void input() {
    N = scan.nextInt();
    M = scan.nextInt();
    A = new int[N + 1];
    for (int i = 1; i <= N; i++) {
      A[i] = scan.nextInt();
    }
  }

  private static boolean determination(final int money) {
    int cnt = 1, sum = 0;
    for (int i = 1; i <= N; i++) {
      if (sum + A[i] > money) {
        cnt++;
        sum = A[i];
      } else {
        sum += A[i];
      }
    }

    return cnt <= M;
  }

  static void pro() {
    int L = A[1], R = 1000000000, ans = 0;
    for (int i = 1; i <= N; i++) L = Math.max(L, A[i]);  // 적어도 하루에 쓸 돈의 최댓값 만큼은 인출해야 한다!
    // [L ... R] 범위 안에 정답이 존재한다!
//    int L = 1, R = 100_000, ans = 0; // 안되면 이거
    while (L <= R) {
      int mid = (L + R) / 2;
      if (determination(mid)) {
        R = mid - 1;
        ans = mid;
      } else {
        L = mid + 1;
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