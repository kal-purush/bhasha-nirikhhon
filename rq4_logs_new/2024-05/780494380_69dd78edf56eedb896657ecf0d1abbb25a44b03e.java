package org.example.spanningtree;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class back14621 {
  static FastReader scan = new FastReader();
  static int N, M, ans;
  static String[] school_type;
  static int[] parents;
  static List<Edge> edges = new ArrayList<>();

  /*
5 7
M W W W M
1 2 12
1 3 10
4 2 5
5 2 5
2 5 10
3 4 3
5 4 7

   */
  static void input() {
    N = scan.nextInt();
    M = scan.nextInt();

    school_type = new String[N + 1];
    parents = new int[N + 1];
    for (int i = 0; i <= N; i++) {
      parents[i] = i;
    }

    for (int i = 1; i <= N; i++) {
      school_type[i] = scan.next();
    }

    for (int i = 1; i <= M; i++) {
      int start = scan.nextInt();
      int end = scan.nextInt();
      int weight = scan.nextInt();

      // 남 - 남 // 여 - 여 검사
      if (school_type[start].equals(school_type[end])) continue;

      edges.add(new Edge(start, end, weight));
    }

  }

  static int find(int root) {
    if (parents[root] == root) return parents[root];

    return parents[root] = find(parents[root]);
  }

  static void union(int a, int b) {
    int rootA = find(a);
    int rootB = find(b);

    if (rootA != rootB) {
      if (rootA < rootB)
        parents[rootB] = rootA;
      else
        parents[rootA] = rootB;
    }
  }

  static void pro() {
//    for (Edge edge : edges) {
//      System.out.println(edge.start + " " + edge.end + " " + edge.weight);
//    }
    Collections.sort(edges);
//    System.out.println();
//    for (Edge edge : edges) {
//      System.out.println(edge.start + " " + edge.end + " " + edge.weight);
//    }

    MST();

    // find(i)을 이용해 모든 root가 연결이 안되면 -1
    int first_root = find(parents[1]);
    boolean check = true;
    for (int i = 2; i <= N; i++) {
      int second_root = find(parents[i]);

      if (first_root != second_root) {
        check = false;
        break;
      }
      first_root = second_root;
    }

    if (check) {
      System.out.println(ans);
    } else {
      System.out.println(-1);
    }
  }

  private static void MST() {
    for (Edge edge : edges) {
      int start = edge.start, end = edge.end;
      int root_start = find(start), root_end = find(end);
      if (root_start != root_end) {
        union(root_start, root_end);
        ans += edge.weight;
      }
//      System.out.println(Arrays.toString(parents));
//      System.out.println(start + " " + end);
//      System.out.println(root_start + " " + root_end);
    }
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  static void print(int[][] graph) {
    for (int i = 1; i <= N; i++) {
      for (int j = 1; j <= M; j++) {
        System.out.print(graph[i][j] + " ");
      }
      System.out.println();
    }
  }

  static class Edge implements Comparable<Edge> {
    int start, end;
    int weight;

    public Edge(int start, int end, int weight) {
      this.start = start;
      this.end = end;
      this.weight = weight;
    }

    @Override
    public int compareTo(final Edge o) {
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