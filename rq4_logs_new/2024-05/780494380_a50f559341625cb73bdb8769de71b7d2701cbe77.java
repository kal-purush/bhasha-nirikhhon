package org.example.exhaustive_search;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

public class back16987 {

  /*
3
8 5
1 100
3 5
   */
  static int N, ans = 0;
  static int[][] egg;
  static BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

  private static void input() throws IOException {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    StringTokenizer st = null;

    N = Integer.parseInt(br.readLine());
    egg = new int[N][2]; // 내구도, 무게

    for (int i = 0; i < N; i++) {
      st = new StringTokenizer(br.readLine());
      egg[i][0] = Integer.parseInt(st.nextToken());
      egg[i][1] = Integer.parseInt(st.nextToken());
    }
  }

  private static void pro() {
    solution(0, 0);
    System.out.println(ans);
  }

  // 손에 계란으로 다음 계란을 치는 행동
  private static void solution(int now, int breakEgg) {
    ans = Math.max(ans, breakEgg);
    if (now == N) {
      return; // 가장 최근에 든 계란이 가장 오른쪽에 위치한 계란
    }

    if (egg[now][0] <= 0) { // 손안의 계란이 깨진다.
      solution(now + 1, breakEgg);
    } else {
      for (int next = 0; next < N; next++) {
        if (next == now || egg[next][0] <= 0) {
          continue; // 깨지지 않은 다른 계란을 치기 위해
        }

        egg[now][0] -= egg[next][1];
        egg[next][0] -= egg[now][1];

        int newBreakEgg = breakEgg;
        if (egg[now][0] <= 0) {
          newBreakEgg++;
        }
        if (egg[next][0] <= 0) {
          newBreakEgg++;
        }

        solution(now + 1, newBreakEgg);
        egg[now][0] += egg[next][1];
        egg[next][0] += egg[now][1];
      }
    }
  }

  public static void main(String[] args) throws IOException {
    input();
    pro();
  }

  private static void print(int[][] arr) {
    for (final int[] ints : arr) {
      System.out.print(Arrays.toString(ints) + " ");
    }
  }
}