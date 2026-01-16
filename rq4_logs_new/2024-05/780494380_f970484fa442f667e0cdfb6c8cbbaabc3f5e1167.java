package org.example.two_pointer;

import java.io.*;
import java.util.Arrays;
import java.util.StringTokenizer;

public class back2467 {
  /*
5
-99 -2 -1 4 98

5
-99 -2 -1 4 99


   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N;
  static int[] arr;

  static void input() {
    N = scan.nextInt();
    arr = new int[N + 1];

    for (int i = 1; i <= N; i++) {
      arr[i] = scan.nextInt();
    }

  }

  static void pro() {
    long MIN = Long.MAX_VALUE;
    long sum = 0;
    int L = 1, R = N;
    int ans_a = 0, ans_b = 0;

    while (L < R) {
      sum = arr[L] + arr[R];

      // 결과 최솟값일때 정답 갱신
      if (MIN > Math.abs(sum)) {
        MIN = Math.abs(sum);
//        System.out.println(sum);
        ans_a = arr[L];
        ans_b = arr[R];
//        System.out.println(ans_a + " " + ans_b);
      }

      // sum > 0
      if (sum > 0) {
//        System.out.println("sum>0 " + arr[L] + " " + arr[R]);
        R--;
      }
      // sum == 0
      else if (sum == 0) {
        ans_a = arr[L];
        ans_b = arr[R];
        break;
      }
      // sum < 0
      else {
//        System.out.println("sum<0 " + arr[L] + " " + arr[R]);
        L++;
      }
    }

    System.out.println(ans_a + " " + ans_b);
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  static void print(int[] arr) {
    System.out.println(Arrays.toString(arr));
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