import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class Baekjun9177 {
	private static char[] s1;
	private static char[] s2;
	private static char[] s3;
	private static int l1;
	private static int l2;
	private static int l3;

	public static void main(String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		int TC = Integer.parseInt(br.readLine());
		StringBuilder sb = new StringBuilder();
		for (int tc = 1; tc <= TC; tc++) {
			sb.append("Data set ").append(tc).append(": ");
			StringTokenizer st = new StringTokenizer(br.readLine());
			s1 = st.nextToken().toCharArray();
			s2 = st.nextToken().toCharArray();
			s3 = st.nextToken().toCharArray();

			l1 = s1.length;
			l2 = s2.length;
			l3 = s3.length;
			boolean[][] dp = new boolean[l1+1][l2+1];
			dp[0][0]=true;
			for (int i = 0; i < l3; i++) {
				for (int x = (0>i-l2?0:i-l2); x <= (l1>i?i:l1); x++) {
					int y = i-x;
					if (!dp[x][y]) {
						continue;
					}
					if (x<l1 && s1[x]==s3[i]) {
						dp[x+1][y]=true;
					}
					if (y<l2 && s2[y]==s3[i]) {
						dp[x][y+1]=true;
					}
				}
			}
			if (dp[l1][l2]) {
				sb.append("yes");				
			} else {
				sb.append("no");
			}
			sb.append("\n");
		}
		System.out.println(sb);
	}
}