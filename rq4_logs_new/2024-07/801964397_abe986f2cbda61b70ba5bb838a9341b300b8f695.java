import java.io.*;
import java.util.*;

public class code20444 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st = new StringTokenizer(br.readLine());
		long n = Long.parseLong(st.nextToken());
		long k = Long.parseLong(st.nextToken());

	// 	잘라서 나오는 종이의 갯수
		long left = 0;
		long right = n/2;

		while (left <= right) {
			long mid = (left + right) / 2;

			long nums = (mid + 1) * (n - mid + 1);
			if (nums == k) {
				System.out.println("YES");
				return;
			} else if (nums < k) {
				left = mid + 1;
			} else
				right = mid - 1;
		}
		System.out.println("NO");
	}
}