import java.io.*;
import java.util.*;

class Main {
  static int[][] dp;
  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    int N = Integer.parseInt(br.readLine());

    dp = new int[10][N+1];

    for(int i=0; i<10; ++i) {
      dp[i][1] = 1;
    }
    for(int i=1; i<=N; ++i) { // 0으로 끝나는 오르막수는 항상 1개이다.
      dp[0][i] = 1;
    }

    for(int i=2; i<=N; ++i) { // 몇자리수인지?
      for(int j=1; j<10; ++j) { // 맨 끝 숫자
        dp[j][i] =  (dp[j-1][i] + dp[j][i-1]) % 10007;
      }
    }

    int ans = 0;
    for(int i=0; i<10; ++i) {
      ans += dp[i][N];
      ans %= 10007;
    }

    System.out.println(ans);
  }
}