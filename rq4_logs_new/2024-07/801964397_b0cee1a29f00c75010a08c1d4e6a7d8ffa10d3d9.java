import java.io.*;
import java.util.*;

// 9465.스티커
public class code9465 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st;
		StringBuilder sb = new StringBuilder();
		int t = Integer.parseInt(br.readLine());

		while (t-- > 0) {
			int n = Integer.parseInt(br.readLine());

			// 스티커 초기화
			int[][] stickers = new int[2][n + 1];
			int[][] dp = new int[2][n + 1];
			for (int i = 0; i < 2; i++) {
				st = new StringTokenizer(br.readLine());
				for (int j = 1; j <= n; j++) {
					stickers[i][j] = Integer.parseInt(st.nextToken());
				}
			}

			dp[0][1] = stickers[0][1];
			dp[1][1] = stickers[1][1];

			for (int j = 2; j <= n; j++) {
				dp[0][j] = Math.max(dp[1][j - 1], dp[1][j - 2]) + stickers[0][j];
				dp[1][j] = Math.max(dp[0][j - 1], dp[0][j - 2]) + stickers[1][j];
			}
			sb.append(Math.max(dp[0][n], dp[1][n])).append("\n");
		}
		System.out.println(sb);
	}
}