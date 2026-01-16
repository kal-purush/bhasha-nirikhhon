package org.example.tree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class back15900 {
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N;
  static List<Integer>[] adj;
  static int[] leaf;
  static int total_sum_depth;

  /*
8
8 1
1 4
7 4
6 4
6 5
1 3
2 3

   */
  static void input() throws IOException {
    N = scan.nextInt();
    adj = new ArrayList[N + 1];
    for (int i = 1; i <= N; i++) adj[i] = new ArrayList<>();
    leaf = new int[N + 1];

    for (int i = 1; i <= N - 1; i++) {
      int a = scan.nextInt(), b = scan.nextInt();
      adj[a].add(b);
      adj[b].add(a);
    }
    int i = 0;
  }

  private static void dfs(final int x, final int par, final int depth) {
    if (adj[x].size() == 1) total_sum_depth += depth;

    for (Integer value : adj[x]) {
      if (value == par) continue;
      dfs(value, x, depth + 1);
    }
  }

  static void pro() {
    dfs(1, -1, 0);

    if (total_sum_depth % 2 == 0) {
      System.out.println("No");
    } else {
      System.out.println("Yes");
    }
  }

  public static void main(String[] args) throws IOException {
    input();
    pro();
  }

  public static class Node {
    int root;
    Node left;
    Node right;

    public Node(int root) {
      this.root = root;
    }

    public void insert(int value) {
      if (this.root < value) {
        if (this.left == null) {
          this.left = new Node(value);
        } else {
          this.left.insert(value);
        }
      } else {
        if (this.right == null) {
          this.right = new Node(value);
        } else {
          this.right.insert(value);
        }
      }
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