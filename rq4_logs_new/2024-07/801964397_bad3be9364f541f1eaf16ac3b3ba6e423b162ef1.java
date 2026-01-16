import java.io.*;
import java.util.*;

// 4781.사탕 가게
public class code4781 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st;
		StringBuilder sb = new StringBuilder();

		int[] calories;
		int[] prices;
		int[] dp;

		while (true) {
			st = new StringTokenizer(br.readLine());
			int n = Integer.parseInt(st.nextToken());
			int m = (int)(Float.parseFloat(st.nextToken()) * 100 + 0.5);

			if (n == 0 && m == 0f) {
				// 종료 조건
				break;
			}
			calories = new int[n + 1];
			prices = new int[n + 1];
			for (int i = 1; i <= n; i++) {
				st = new StringTokenizer(br.readLine());
				calories[i] = Integer.parseInt(st.nextToken());
				prices[i] = (int)(Float.parseFloat(st.nextToken()) * 100 + 0.5);
			}

			dp = new int[m + 1];
			for (int i = 1; i <= n; i++) {
				for (int j = 0; j <= m; j++) {
					if (j - prices[i] >= 0) {
						dp[j] = Math.max(dp[j], dp[j - prices[i]] + calories[i]);
					}
				}
			}
			sb.append(dp[m]).append("\n");
		}
		System.out.println(sb);
	}
}