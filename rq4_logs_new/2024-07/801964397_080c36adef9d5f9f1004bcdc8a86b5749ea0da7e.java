import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class code19637 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		StringBuilder sb = new StringBuilder();

		int n = Integer.parseInt(st.nextToken());
		int m = Integer.parseInt(st.nextToken());

		String[] title = new String[n];
		int[] power = new int[n];
		for (int i = 0; i < n; i++) {
			st = new StringTokenizer(br.readLine());
			title[i] = st.nextToken();
			power[i] = Integer.parseInt(st.nextToken());
		}

		for (int i = 0; i < m; i++) {
			int inputNum = Integer.parseInt(br.readLine());
			int start = 0;
			int end = n-1;

			while (start <= end) {
				int mid = (start + end) / 2;

				if (power[mid] < inputNum) {
					start = mid + 1;
				}
				else end = mid - 1;
			}
			sb.append(title[start]).append("\n");
		}
		System.out.println(sb);

	}
}