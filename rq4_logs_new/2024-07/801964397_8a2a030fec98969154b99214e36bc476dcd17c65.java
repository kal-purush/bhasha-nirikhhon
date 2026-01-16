import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.StringTokenizer;

public class code1931 {
	public static void main(String[] args) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		StringTokenizer st;
		int n = Integer.parseInt(br.readLine());	//n개의 회의

		// 입력
		int[][] meetings = new int[n][2];
		for (int i = 0; i < n; i++) {
			st = new StringTokenizer(br.readLine());
			meetings[i][0] =Integer.parseInt(st.nextToken());	// 시작 시간
			meetings[i][1] =Integer.parseInt(st.nextToken());	// 끝나는 시간
		}

		Arrays.sort(meetings, new Comparator<int[]> (){
			// 끝나는 시간 기준으로 오름차순 정렬
			// 끝나는 시간이 같다면 시작 시간 기준 오름차순
			@Override
			public int compare(int[] o1, int[] o2) {
				if (o1[1] == o2[1]) {
					return o1[0] - o2[0];
				}
				return o1[1] - o2[1];
			}
		});

		int cnt = 0;
		int time = 0;
		for (int i = 0; i < n; i++) {
			if (time < meetings[i][0]) {
				time = meetings[i][1];
				cnt++;
			}
		}
		System.out.println(cnt);
	}
}