package org.example.union;

import java.io.*;
import java.util.StringTokenizer;

public class back1717 {
  /*
7 8
0 1 3
1 1 7
0 7 6
1 7 1
0 3 7
0 4 2
0 1 1
1 1 1

1 2
1 0 1
1 1 1

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N, M;
  static int[] parents;
  static int[][] inputs;

  static void input() {
    N = scan.nextInt();
    M = scan.nextInt();

    parents = new int[N + 1];
    for (int i = 0; i <= N; i++) {
      parents[i] = i;
    }
    inputs = new int[M + 1][3];

    for (int i = 1; i <= M; i++) {
      int kind = scan.nextInt();
      int a = scan.nextInt();
      int b = scan.nextInt();

      inputs[i] = new int[]{kind, a, b};
    }
  }

  static void pro() {
//    print();
    for (int i = 1; i <= M; i++) {
      int[] input = inputs[i];
      int kind = input[0];
      int a = input[1];
      int b = input[2];

      if (kind == 0) { // 합집합
//        union(Math.max(a, b), Math.min(a, b));
        union(a, b);
        System.out.println("union: " + a + " -> " + b + ": ");
        print();
        System.out.println();
      }

      if (kind == 1) { // 같은 집합 확인
        int root_a = find(a);
        int root_b = find(b);

        if (root_a == root_b)
          sb.append("YES").append("\n");
        else
          sb.append("NO").append("\n");
      }
    }
    System.out.println(sb);
  }

  private static void union(final int a, final int b) {
    int root_a = find(a);
    int root_b = find(b);
//    System.out.println("a: " + a + " root_a: " + root_a + " b: " + b + " root_b: " + root_b);

    if (root_a != root_b) {
      if (root_a < root_b) {
        parents[root_b] = root_a;
      } else {
        parents[root_a] = root_b;
      }
    }
  }

  // root을 찾는다.
  private static int find(final int root) {
    if (parents[root] == root)
      return parents[root];
    return parents[root] = find(parents[root]);
  }

  private static void print() {
    for (int i = 0; i <= N; i++) {
      System.out.print(i + " | ");
    }
    System.out.println();
    for (int i = 0; i <= N; i++) {
      System.out.print(parents[i] + " | ");
    }
    System.out.println();
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