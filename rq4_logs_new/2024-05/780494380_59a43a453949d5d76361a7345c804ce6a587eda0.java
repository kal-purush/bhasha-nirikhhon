package org.example.dp;

import java.io.*;
import java.util.Arrays;
import java.util.StringTokenizer;

public class back16724 {
  /*
3 4
DLLL
DRLU
RRRU

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int[][] visited;
  static int N, M, idx, cnt;
  static char[][] arr;

  static void input() {
    N = scan.nextInt();
    M = scan.nextInt();
    arr = new char[N][M];
    visited = new int[N][M];

    for (int i = 0; i < N; i++) {
      String in = scan.nextLine();

      for (int j = 0; j < M; j++) {
        arr[i][j] = in.charAt(j);
      }
    }
//    for (char[] chars : arr) {
//      System.out.println(Arrays.toString(chars));
//    }
  }

  private static void dfs(final int x, final int y) {

    int[] move = dir(arr[x][y]);
    visited[x][y] = idx;

    int nx = x + move[0];
    int ny = y + move[1];

//    System.out.println(arr[x][y] + " " + x + " " + y + " " + nx + " " + ny);
//    print();
//    System.out.println();
    if (visited[nx][ny] == 0) {
      dfs(nx, ny);
    } else {
      if (visited[nx][ny] == idx) {
//        System.out.println(arr[x][y] + " " + x + " " + y + " " + nx + " " + ny);
//        System.out.println(visited[nx][ny] + " " + idx);
//        System.out.println();
        cnt++;
//        System.out.println("cnt: " + cnt);
      }
      // 사이클
      idx++;
//      System.out.println("idx: " + idx);
    }
  }

  static void pro() {
    idx = 1;

    for (int i = 0; i < N; i++) {
      for (int j = 0; j < M; j++) {
        if (visited[i][j] == 0) {
          dfs(i, j);
        }
      }
    }

    System.out.println(cnt);
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  public static void print() {
    for (int[] ints : visited) {
      System.out.println(Arrays.toString(ints));
    }
  }

  static int[] dir(char dir) {
//    U, D, L, R
    switch (dir) {
      case 'U':
        return new int[]{-1, 0};
      case 'D':
        return new int[]{1, 0};
      case 'L':
        return new int[]{0, -1};
      case 'R':
        return new int[]{0, 1};
    }

    return null;
  }


  static void print(int[] par) {
    System.out.println(Arrays.toString(par));
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