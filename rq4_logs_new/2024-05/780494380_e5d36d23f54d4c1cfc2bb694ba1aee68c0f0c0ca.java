package org.example.spanningtree;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class back1647 {
  /*
7 12
1 2 3
1 3 2
3 2 1
2 5 2
3 4 4
7 3 6
5 1 5
1 6 2
6 4 1
6 5 3
4 5 3
6 7 4

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N, M, ans;
  static int[] par;
  static List<Node> nodes;
  static List<Integer> results;

  static void input() {
    N = scan.nextInt();
    M = scan.nextInt();
    nodes = new ArrayList<>();
    par = new int[N + 1];
    results = new ArrayList<>();
    for (int i = 1; i <= N; i++) {
      par[i] = i;
    }

    for (int i = 0; i < M; i++) {
      nodes.add(new Node(scan.nextInt(), scan.nextInt(), scan.nextInt()));
    }
  }

  static void pro() {
    Collections.sort(nodes);

    kruskal();

    int ans = 0;
    for (int i = 0; i < results.size() - 1; i++) {
      ans += results.get(i);
    }

    System.out.println(ans);
  }

  public static void kruskal() {
    for (Node node : nodes) {
      int a_root = find(node.a);
      int b_root = find(node.b);

      if (a_root == b_root) continue;

      union(a_root, b_root);
      results.add(node.weight);
    }
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  public static int find(int root) {
    if (par[root] == root) {
      return par[root];
    }

    return par[root] = find(par[root]);
  }

  public static void union(int a, int b) {
    int a_root = find(a);
    int b_root = find(b);

    if (a_root != b_root) {
      if (a_root < b_root)
        par[b_root] = a_root;
      else
        par[a_root] = b_root;
    }
  }

  static class Node implements Comparable<Node> {
    int a, b;
    int weight;

    public Node(int a, int b, int weight) {
      this.a = a;
      this.b = b;
      this.weight = weight;
    }

    public int compareTo(Node o) {
      if (this.weight != o.weight) {
        return this.weight - o.weight;
      }

      return 0;
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