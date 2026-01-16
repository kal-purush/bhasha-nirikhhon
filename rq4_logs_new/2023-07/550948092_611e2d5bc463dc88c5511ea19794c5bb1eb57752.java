package DoIt.DynamicProgramming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/*
    Problem 085 테스트 케이스 1-3만 통과
    문제점: 조건을 만족하는 남은 날 중에서 최댓값을 만들 수 있는 날이 아니라 그냥 순서대로 일정 책
 */
public class Solution2 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int[][] arr = new int[n][2];
        for (int i = 0; i < n; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine());
            arr[i][0] = Integer.parseInt(st.nextToken());
            arr[i][1] = Integer.parseInt(st.nextToken());
        }
        int max = 0;
        for (int i = 0; i < n / 2; i++) {
            int pay = 0;
            while (i < n) {
                if (arr[i][0] > n - i) {
                    break;
                }
                pay += arr[i][1];
                i += arr[i][0];
            }
            max = Math.max(max, pay);
        }
        System.out.println(max);
    }
}