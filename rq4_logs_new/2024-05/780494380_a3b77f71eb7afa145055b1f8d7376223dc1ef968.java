package org.example.topological;

import java.io.*;
import java.util.*;

public class back1516 {
  /*
5
10 -1
10 1 -1
4 1 -1
4 3 1 -1
3 3 -1

5
10 -1
10 1 -1
4 1 -1
4 3 1 5 -1
3 3 -1


   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();

  static int N;
  static int[] indeg, seconds;
  static long[] board;
  static ArrayList<Integer>[] adj;

  static void input() {
    N = scan.nextInt();
    adj = new ArrayList[N + 1];
    indeg = new int[N + 1];
    seconds = new int[N + 1];
    board = new long[N + 1];
    for (int i = 1; i <= N; i++)
      adj[i] = new ArrayList<>();
    int in = 0;
    for (int i = 1; i <= N; i++) {
      int second = scan.nextInt();
      seconds[i] = second;
      while ((in = scan.nextInt()) != -1) {
        adj[in].add(i);
        indeg[i]++;
      }
    }
//    System.out.println("seconds " + Arrays.toString(seconds));
//    int i = 0;
//    for (ArrayList<Integer> integers : adj) {
//      System.out.println(i++ + " " + integers);
//    }
//    System.out.println("indeg " + Arrays.toString(indeg));
//    System.out.println();
  }

  static void pro() {
    Queue<Integer> q = new LinkedList<>();

    for (int i = 1; i <= N; i++) {
      if (indeg[i] == 0) {
        q.add(i);
        board[i] = seconds[i];
      }
    }

    while (!q.isEmpty()) {
      int x = q.poll();

      for (Integer value : adj[x]) {
        board[value] = Math.max(board[value], board[x] + seconds[value]);
        indeg[value]--;
        if (indeg[value] == 0) {
          q.add(value);
        }
      }
    }

    for (int i = 1; i <= N; i++) {
      sb.append(board[i]).append("\n");
    }
    System.out.println(sb);
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  static void print(int[] arr) {
    System.out.println(Arrays.toString(arr));
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