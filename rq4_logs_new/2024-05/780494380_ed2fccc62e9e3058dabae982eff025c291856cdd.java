package org.example.tree;

import java.io.*;
import java.util.StringTokenizer;

public class back3584 {
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N;
  static boolean[] visited;
  static int[] pars;

  /*
2
16
1 14
8 5
10 16
5 9
4 6
8 4
4 10
1 13
6 15
10 11
6 7
10 2
16 3
8 1
16 12
16 7
5
2 3
3 4
3 1
1 5
3 5

5
2 3
3 4
3 1
1 5
3 5

   */
  static void input() throws IOException {
    N = scan.nextInt();
    visited = new boolean[N + 1];
    pars = new int[N + 1];

    for (int i = 1; i <= N - 1; i++) {
      int par = scan.nextInt();
      int child = scan.nextInt();
      pars[child] = par;
    }
  }

  static void pro() {
    int one = scan.nextInt(), two = scan.nextInt();

    while (one > 0) {
      visited[one] = true;
      one = pars[one];
    }

    while (!visited[two] && two > 0) {
      two = pars[two];
    }

    sb.append(two).append("\n");
  }

  public static void main(String[] args) throws IOException {
    int T = scan.nextInt();

    for (int i = 0; i < T; i++) {
      input();
      pro();
    }

    System.out.println(sb);
  }

  static class Node {
    int root;
    int value;
    boolean visited;

    public Node(int root, int value) {
      this.root = root;
      this.value = value;
      this.visited = false;
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