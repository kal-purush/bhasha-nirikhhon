package org.example.union;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class back1043 {
  /*
10 9
4 1 2 3 4
2 1 5
2 2 6
1 7
1 8
2 7 8
1 9
1 10
2 3 10
1 4

10 9
4 1 2 3 4
2 1 5
2 2 6
1 7
1 8
2 7 8
2 9 10
1 10
2 3 10
1 4

1 1
1 1
1 1

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N, M, K;
  static int[] parents, truths;
  static int[][] inputs;
  static List<Integer>[] party_persons;

  static void input() {
    N = scan.nextInt();
    M = scan.nextInt();
    K = scan.nextInt();

    if (K == 0) {
      System.out.println(M);
      System.exit(0);
    }

    // 파티에 참가하는 사람
    party_persons = new ArrayList[M + 1];
    for (int i = 0; i <= M; i++) {
      party_persons[i] = new ArrayList<>();
    }

    // 부분 집합
    parents = new int[N + 1];
    for (int i = 0; i <= N; i++) {
      parents[i] = i;
    }

    // 진실을 아는사람 모임
    truths = new int[K + 1];
    for (int i = 1; i <= K; i++) {
      truths[i] = scan.nextInt();
    }

    // 파티에 참여하는 사람
    for (int party_idx = 1; party_idx <= M; party_idx++) {
      int party_person_cnt = scan.nextInt();

      for (int j = 1; j <= party_person_cnt; j++) {
        party_persons[party_idx].add(scan.nextInt());
      }
    }
  }

  static void pro() {
    int ans = 0; // 정답의 파티의 수
    // 진실을 아는 사람들 Union
    int truth_root = truths[1];
    for (int i = 2; i <= K; i++) {
      union(truth_root, truths[i]);
    }

    // 파티에 참가하는 사람 union
    for (int i = 1; i < party_persons.length; i++) {
      int party_person_first = party_persons[i].get(0);

      for (int j = 1; j < party_persons[i].size(); j++) {
        union(party_person_first, party_persons[i].get(j));
        print(party_person_first, party_persons[i].get(j));
      }
    }

//    print();

    // 과장되서 말할 수 있는 파티 선택
    for (int i = 1; i < party_persons.length; i++) {
      int party_person_first = party_persons[i].get(0);
      int party_person_root = find(party_person_first);

      boolean truth_contain = contain(truths, party_person_root);
      if (!truth_contain)
        ans++;
    }

//    print();
    System.out.println(ans);
    System.out.println(Arrays.toString(truths));
  }

  private static boolean contain(final int[] truths, final int partyPersonRoot) {
    int truth_root = find(truths[1]);

    return truth_root == partyPersonRoot;
  }

  private static void union(final int a, final int b) {
    int a_root = find(a);
    int b_root = find(b);

    if (a_root != b_root) {
      if (a_root < b_root) {
        parents[b_root] = a_root;
      } else {
        parents[a_root] = b_root;
      }
    }
  }

  // root을 찾는다.
  private static int find(final int root) {
    if (parents[root] == root)
      return parents[root];

    return parents[root] = find(parents[root]);
  }

  public static void main(String[] args) {
    input();
    pro();
  }

  private static void print() {
    for (int i = 0; i <= N; i++) {
      System.out.print(i + " | ");
    }
    System.out.println();
    for (int i = 0; i <= N; i++) {
      System.out.print(parents[i] + " | ");
    }
    System.out.println();
  }

  private static void print(int a, int b) {
    System.out.println(a + " -> " + b);
    for (int i = 0; i <= N; i++) {
      System.out.print(i + " | ");
    }
    System.out.println();
    for (int i = 0; i <= N; i++) {
      System.out.print(parents[i] + " | ");
    }
    System.out.println();
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