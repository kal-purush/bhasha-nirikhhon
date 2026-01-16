import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

public class code11399 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st;
		int n = Integer.parseInt(br.readLine());

		// 입력
		int[] times = new int[n];
		st = new StringTokenizer(br.readLine());
		for (int i = 0; i < n; i++) {
			times[i] = Integer.parseInt(st.nextToken());
		}

		Arrays.sort(times);    //오름차순 정렬
		int result = 0;
		int cnt = n;
		for (int i = 0; i < n; i++) {
			result += (times[i] * cnt);
			cnt--;
		}
		System.out.println(result);
	}
}