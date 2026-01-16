import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class code14501 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		int n = Integer.parseInt(br.readLine());

		int[] times = new int[n];
		int[] prices = new int[n];
		int[] dp = new int[n + 1];

		StringTokenizer st;
		for (int i = 0; i < n; i++) {
			st = new StringTokenizer(br.readLine());
			times[i] = Integer.parseInt(st.nextToken());
			prices[i] = Integer.parseInt(st.nextToken());
		}

		for (int i = 0; i < n; i++) {
			if (i + times[i] < n) {
				// 내가 이번 상담을 선택하느냐, 선택하지 않느냐
				dp[i + times[i]] = Math.max(dp[i + times[i]], dp[i] + prices[i]);
			}else{
				dp[i + 1] = Math.max(dp[i], dp[i + 1]);
			}
		}
		System.out.println(dp[n]);
	}
}