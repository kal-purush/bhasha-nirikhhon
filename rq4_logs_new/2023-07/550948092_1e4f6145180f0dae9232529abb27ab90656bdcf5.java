package DoIt.DynamicProgramming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/*
    085 퇴사 준비하기
    점화식 활용하기
    D[i]: i번째 날부터 퇴사날까지 벌 수 있는 '최대' 수입

 */
public class Solution2_2 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int[][] arr = new int[n+1][2];
        int[] D = new int[n+2];
        for (int i = 1; i <= n; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine());
            arr[i][0] = Integer.parseInt(st.nextToken());
            arr[i][1] = Integer.parseInt(st.nextToken());
        }
        for(int i = n; i > 0; i--){
            if(i + arr[i][0] > n+1) {
                D[i] = D[i + 1];
            } else {
                D[i] = Math.max(D[i+1], arr[i][1] + D[i+arr[i][0]]);
            }
        }
        System.out.println(D[1]);
    }
}