package org.example.exhaustive_search;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

public class back11725 {
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();

  static int n;
  static ArrayList<Integer>[] adj;
  static int[] parent;

  /*
7
1 6
6 3
3 5
4 1
2 4
4 7

   */
  static void input() {
    n = scan.nextInt();
    parent = new int[n + 1];
    Arrays.fill(parent, -1);
    adj = new ArrayList[n + 1];
    for (int i = 1; i <= n; i++) {
      adj[i] = new ArrayList<>();
    }
    // 인접 리스트 구성하기
    /* TODO */
    for (int i = 0; i < n - 1; i++) {
      int a = scan.nextInt();
      int b = scan.nextInt();
      adj[a].add(b);
      adj[b].add(a);
    }
  }

  // dfs(x, par) := 정점 x 의 부모가 par 였고, x의 children들을 찾아주는 함수
  static void dfs(int x, int par) {
    /* TODO */
    parent[x] = par;
    for (Integer value : adj[x]) {
      if (parent[value] == -1) {
        dfs(value, x);
      }
    }
  }

  static void pro() {
    // 1 번 정점이 ROOT 이므로, 여기서 시작해서 Tree의 구조를 파악하자.
    /* TODO */
    dfs(1, 1);

    // 정답 출력하기
    /* TODO */
    for (int i = 2; i <= n; i++) {
      sb.append(parent[i]).append("\n");
    }

    System.out.println(sb);
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