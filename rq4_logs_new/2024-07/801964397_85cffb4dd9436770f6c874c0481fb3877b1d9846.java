import java.util.*;
import java.io.*;

public class code2776 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		int t = Integer.parseInt(br.readLine());

		for (int i = 0; i < t; i++) {
			Set<Integer> set = new HashSet<>();
			StringBuilder sb = new StringBuilder();
			int n = Integer.parseInt(br.readLine());
			StringTokenizer st = new StringTokenizer(br.readLine());
			for (int j = 0; j < n; j++){
				set.add(Integer.parseInt(st.nextToken()));
			}
			int m = Integer.parseInt(br.readLine());
			st = new StringTokenizer(br.readLine());
			for (int j = 0; j < m; j++) {
				sb.append(set.contains(Integer.parseInt(st.nextToken())) ? 1 : 0).append("\n");
			}
			System.out.print(sb);
		}
	}
}