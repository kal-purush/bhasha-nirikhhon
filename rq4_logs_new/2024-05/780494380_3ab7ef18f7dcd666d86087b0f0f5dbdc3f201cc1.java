package org.example.dijkstra;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class back1167 {
  /*
12
1 2 3
1 3 2
2 4 5
3 5 11
3 6 9
4 7 1
4 8 7
5 9 15
5 10 4
6 11 6
6 12 10

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N;
  static int[] dists;
  static List<Node>[] nodes;
  static int MAX = Integer.MIN_VALUE;
  static boolean[] visited;
  static List<Integer> leafNode;
  static int max_node;

  static void input() {
    N = scan.nextInt();
    dists = new int[N + 1];
    nodes = new ArrayList[N + 1];
    visited = new boolean[N + 1];
    leafNode = new ArrayList<>();
    if (N == 1) {
      MAX = 0;
    }
    for (int i = 1; i <= N; i++) nodes[i] = new ArrayList<>();

    for (int i = 1; i <= N - 1; i++) {
      int a = scan.nextInt();
      int b = scan.nextInt();
      int weight = scan.nextInt();

      nodes[a].add(new Node(b, weight));
      nodes[b].add(new Node(a, weight));
    }

    // leafNode 구하기
    for (int i = 1; i <= N; i++) {
      if (nodes[i].size() == 1) {
        leafNode.add(i);
      }
    }
  }

  static void dfs(int start, int weight) {
    visited[start] = true;
    if (MAX < weight) {
      MAX = weight;
      max_node = start;
    }

    for (Node node : nodes[start]) {
      if (visited[node.value]) continue;

      dfs(node.value, weight + node.weigh);
    }
  }

  static void pro() {
    if (N > 1) {

      Arrays.fill(visited, false);
      dfs(1, 0);

      Arrays.fill(visited, false);
      dfs(max_node, 0);
    }
    System.out.println(MAX);
  }

  public static void main(String[] args) {
    input();
    pro();
  }


  static class Node {
    int value;
    int weigh;

    public Node(int value, int weigh) {
      this.value = value;
      this.weigh = weigh;
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