import java.util.Arrays;
import java.util.Scanner;

public class code2217 {
	public static void main(String[] args) {
		Scanner sc = new Scanner(System.in);
		int n = sc.nextInt();
		int[] weights = new int[n];

		for (int i = 0; i < n; i++) {
			weights[i] = sc.nextInt();
		}
		Arrays.sort(weights);	// 내림차순 정렬 필요 -> 최대값을 구해야하니깐

		int cnt = 0;
		int result = Integer.MIN_VALUE;
		for (int i = n - 1; i >= 0; i--) {
			cnt++;
			result = Math.max(result, cnt * weights[i]);
		}
		System.out.println(result);



	}
}