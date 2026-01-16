import java.io.*;
import java.util.*;

// 12865. 평범한 배낭
public class code12865 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		int n = Integer.parseInt(st.nextToken());
		int k = Integer.parseInt(st.nextToken());

		int[] values = new int[n + 1];
		int[] weights = new int[n + 1];

		for (int i = 1; i <= n; i++) {
			st = new StringTokenizer(br.readLine());
			weights[i] = Integer.parseInt(st.nextToken());
			values[i]  = Integer.parseInt(st.nextToken());
		}

		// dp[i][j] : i번째까지 물건을 넣을 수 있고, 배낭에 넣을 수 있는 무게가 j
		int[][] dp = new int[n + 1][k + 1];
		for (int i = 1; i <= n; i++) {
			for (int j = 1; j <= k; j++) {
				if (weights[i] <= j) {
					dp[i][j] = Math.max(dp[i - 1][j], dp[i - 1][j - weights[i]] + values[i]);
				} else {
					dp[i][j] = dp[i - 1][j];
				}
			}
		}

		System.out.println(dp[n][k]);
	}
}