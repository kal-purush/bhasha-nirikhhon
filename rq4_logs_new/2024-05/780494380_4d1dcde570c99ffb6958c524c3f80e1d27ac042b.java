package org.example.exhaustive_search;

import java.io.*;
import java.util.StringTokenizer;


public class back2661 {
  /*
숫자 1, 2, 3 으로만 이루어지는 수열이 있다.
임의의 길이의 인접한 두 개의 부분 수열이 동일한 것이 있으면,
그 수열을 나쁜 수열이라고 부른다. 그렇지 않은 수열은 좋은 수열이다.

다음은 나쁜 수열의 예이다.

33
32121323
123123213
다음은 좋은 수열의 예이다.

2
32
32123
1232123

길이가 N인 좋은 수열들을 N자리의 정수로 보아 그중 가장 작은 수를 나타내는 수열을 구하는 프로그램을 작성하라.
예를 들면, 1213121과 2123212는 모두 좋은 수열이지만 그 중에서 작은 수를 나타내는 수열은 1213121이다.

   */
  static FastReader scan = new FastReader();
  static StringBuilder sb = new StringBuilder();
  static int N;

  static void input() {
//    N = scan.nextInt();
    N = 7;
  }

  private static void recul(final String str) {
    // 재귀함수 종료
    if (str.length() == N) {
      System.out.println(str);
      System.exit(0);
    } else {
      for (int i = 1; i <= 3; i++) {

        // 여기서 같은 부분 수열이 있는지 검사.
        if (valid(str + i)) {
          // 검증에 성공하면 다음 재귀
          recul(str + i);
        }
      }
    }
  }

  // 부분 수열이 있으면 true
  private static boolean valid(final String str) {
    for (int i = 1; i <= str.length() / 2; i++) {
      String first = str.substring(str.length() - i * 2, str.length() - i);
      String second = str.substring(str.length() - i, str.length());

      if (first.equals(second))
        return false;
    }
    return true;
  }

  static void pro() {
    recul("");
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