package org.example.lower_bound;

import java.io.*;
import java.util.StringTokenizer;

public class Basic_lower_bound {


  static int[] A;


  public static void main(String[] args) {
    System.out.println(lower_bound_(A, 1, 1, 1));
  }

  static int lower_bound_(int A[], int L, int R, int X) {
    int result = L - 1;

    while (L <= R) {
      int mid = (L + R) / 2;
      if (A[mid] <= X) {
        L = mid + 1;
      } else {
        R = mid - 1;
      }
    }

    return result;
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