package org.example.spanningtree;

import java.io.*;
import java.util.*;

public class back4386 {
  /*
3
1.0 1.0
2.0 2.0
2.0 4.0

답 : 3.41
   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static Point[] points;
  static double[][] weights;
  static int[] par;
  static int N;
  static List<Edge> edges;
  static double ans;

  static void input() {
    N = scan.nextInt();

    edges = new ArrayList<>();

    par = new int[N + 1];
    for (int i = 0; i <= N; i++) {
      par[i] = i;
    }

    points = new Point[N + 1];
    for (int i = 1; i <= N; i++) {
      double x = scan.nextDouble();
      double y = scan.nextDouble();

      points[i] = new Point(i, x, y);
    }

    weights = new double[N + 1][N + 1];
    for (int i = 1; i <= N; i++) {
      for (int j = 1; j <= N; j++) {
        if (i == j) continue;
        weights[i][j] = distance(points[i], points[j]);
      }
    }
  }

  static void pro() {
    for (int i = 1; i <= N; i++) {
      for (int j = 1; j <= N; j++) {
        if (i == j) continue;
        edges.add(new Edge(points[i].num, points[j].num, weights[i][j]));
      }
    }

    Collections.sort(edges);

    spanniy();

    System.out.printf("%.2f", ans);
  }

  private static void spanniy() {
    for (Edge edge : edges) {
      int a_root = find(edge.start);
      int b_root = find(edge.end);

      if (a_root != b_root) {

        union(a_root, b_root);
        ans += edge.weight;
      }
    }
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  static int find(int root) {
    if (par[root] == root) return par[root];

    return par[root] = find(par[root]);
  }

  static void union(int a, int b) {
    int a_root = find(a);
    int b_root = find(b);

    if (a_root != b_root) {
      if (a_root < b_root)
        par[b_root] = a_root;
      else
        par[a_root] = b_root;
    }
  }

  static void print(int[] par) {
    System.out.println(Arrays.toString(par));
  }

  static double distance(Point p1, Point p2) {
    return Math.sqrt(
        Math.pow(p1.x - p2.x, 2) + Math.pow(p1.y - p2.y, 2)
    );
  }

  static class Point {
    int num;
    double x, y;

    public Point(int num, double x, double y) {
      this.num = num;
      this.x = x;
      this.y = y;
    }
  }

  public static class Edge implements Comparable<Edge> {
    int start, end;
    double weight;

    public Edge(int start, int end, double weight) {
      this.start = start;
      this.end = end;
      this.weight = weight;
    }

    @Override
    public int compareTo(Edge o) {
      if (this.weight > o.weight) {
        return 1;
      }

      return -1;
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