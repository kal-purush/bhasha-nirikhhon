import java.util.*;
import java.io.*;

class Main {
    static int N;

    static int[][] dp;
    static int[][] color;

    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        N = Integer.parseInt(br.readLine());
        dp = new int[4][N+1];
        color = new int[4][N+1];

        for(int i=1; i<=N; ++i) {
            StringTokenizer st = new StringTokenizer(br.readLine());

            for(int j=1; j<=3; ++j) {
                color[j][i] = Integer.parseInt(st.nextToken());
            }
        }

        dp[1][1] = color[1][1]; // 첫번째 집을 R로
        dp[2][1] = color[2][1]; // 첫번째 집을 G로
        dp[3][1] = color[3][1]; // 첫번째 집을 B로


        for(int i=2; i<=N; ++i) {
            dp[1][i] = Math.min(dp[2][i-1], dp[3][i-1]) + color[1][i]; // 현재 집을 R로
            dp[2][i] = Math.min(dp[1][i-1], dp[3][i-1]) + color[2][i]; // 현재 집을 G로
            dp[3][i] = Math.min(dp[1][i-1], dp[2][i-1]) + color[3][i]; // 현재 집을 B로
        }

        int ans = Math.min(dp[1][N], dp[2][N]);
        ans = Math.min(ans, dp[3][N]);

        System.out.println(ans);
    }

}