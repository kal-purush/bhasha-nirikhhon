package org.example.dijkstra;

import java.io.*;
import java.util.*;

public class back1753 {
  /*
5 6
1
5 1 1
1 2 2
1 3 3
2 3 4
2 4 5
3 4 6

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N, M, K;
  static int[] dists;
  static List<Edge>[] edges;

  static void input() {
    N = scan.nextInt();
    M = scan.nextInt();
    K = scan.nextInt();

    dists = new int[N + 1];
    edges = new ArrayList[N + 1];
    for (int i = 1; i <= N; i++)
      edges[i] = new ArrayList<>();

    for (int i = 1; i <= M; i++) {
      int from = scan.nextInt();
      int to = scan.nextInt();
      int weight = scan.nextInt();

      edges[from].add(new Edge(to, weight));
    }
  }

  static void dijkstra(int start) {
    for (int i = 1; i <= N; i++)
      dists[i] = Integer.MAX_VALUE;

    PriorityQueue<Info> q = new PriorityQueue<>(Comparator.comparingInt(o -> o.dist));
    q.add(new Info(start, 0));
    dists[start] = 0;

    while (!q.isEmpty()) {
      Info info = q.poll();

      if (dists[info.idx] != info.dist) continue;

      for (Edge e : edges[info.idx]) {
        if (dists[info.idx] + e.weigh >= dists[e.to]) continue;

        dists[e.to] = dists[info.idx] + e.weigh;
        q.add(new Info(e.to, dists[e.to]));
      }
    }
  }

  static void pro() {
    dijkstra(K);

    for (int i = 1; i <= N; i++) {
      if (dists[i] == Integer.MAX_VALUE)
        sb.append("INF").append("\n");
      else
        sb.append(dists[i]).append("\n");
    }

    System.out.println(sb);
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  static class Info {
    int idx;
    int dist;

    public Info(int idx, int dist) {
      this.idx = idx;
      this.dist = dist;
    }
  }

  static class Edge {
    int to;
    int weigh;

    public Edge(int to, int weigh) {
      this.to = to;
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