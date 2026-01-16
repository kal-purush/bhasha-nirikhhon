import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class code1535 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st, st2;
		int n = Integer.parseInt(br.readLine());

		int[] joys = new int[n];
		int[] healthes = new int[n];
		int[] dp = new int[100];

		st = new StringTokenizer(br.readLine());
		for (int i = 0; i < n; i++) {
			healthes[i] = Integer.parseInt(st.nextToken());
		}

		st = new StringTokenizer(br.readLine());
		for (int i = 0; i < n; i++) {
			joys[i] = Integer.parseInt(st.nextToken());
		}

		for (int i = 0; i < n; i++) {
			for (int j = 99; j >= healthes[i]; j--) {
				dp[j] = Math.max(dp[j - healthes[i]] + joys[i], dp[j]);
			}
		}
		System.out.println(dp[99]);
	}
}