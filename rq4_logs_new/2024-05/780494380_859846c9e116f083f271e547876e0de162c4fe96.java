package org.example.exhaustive_search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Back1015 {

  /*
3
2 3 1

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N;
  static Elen[] B;
  static int[] P;

  /*
4
2 1 3 1

   */
  static void input() {
    N = scan.nextInt();
    B = new Elen[N];
    P = new int[N];
    for (int i = 0; i < N; i++) {
      B[i] = new Elen(scan.nextInt(), i);
    }
  }

  private static void pro() {
    Arrays.sort(B);

    for (int b_idx = 0; b_idx < N; b_idx++) {
      P[B[b_idx].idx] = b_idx;
    }

    for (int i = 0; i < N; i++) {
      sb.append(P[i]).append(" ");
    }
  }

  public static void main(String[] args) {
    input();
    pro();
    System.out.println(sb);
  }

  static class Elen implements Comparable<Elen> {

    int value, idx;

    public Elen(int value, int idx) {
      this.value = value;
      this.idx = idx;
    }

    @Override
    public int compareTo(Elen other) {
      // object sort은 stable 하다.
      return this.value - other.value;
    }
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