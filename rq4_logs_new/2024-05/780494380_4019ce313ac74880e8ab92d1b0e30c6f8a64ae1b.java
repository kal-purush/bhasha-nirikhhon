package org.example.tree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class back14267 {
  /*
9 3
-1 1 2 3 4 2 6 7 4
2 2
3 4
5 6

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N, M;
  static int[] states; // 부모, 칭찬지수
  static List<Integer>[] childs;

  static void input() throws IOException {
    N = scan.nextInt();
    M = scan.nextInt();
    states = new int[N + 1];
    childs = new ArrayList[N + 1];
    for (int i = 1; i <= N; i++) {
      childs[i] = new ArrayList<>();
    }

    // 직속 상관의 부하들
    for (int i = 1; i <= N; i++) {
      int par = scan.nextInt();
      if (i == 1) continue;
      childs[par].add(i);
    }

    // 칭찬 받은 사원의 칭찬 계수 넣기
    for (int i = 0; i < M; i++) {
      int person = scan.nextInt();
      int state = scan.nextInt();
      states[person] += state;
    }
  }

  private static void DFS(final int par) {
    for (Integer child : childs[par]) {
      states[child] += states[par];
      DFS(child);
    }
  }

  static void pro() {


    DFS(1);

    for (int i = 1; i <= N; i++) {
      System.out.print(states[i] + " ");
    }
  }

  public static void main(String[] args) throws IOException {
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