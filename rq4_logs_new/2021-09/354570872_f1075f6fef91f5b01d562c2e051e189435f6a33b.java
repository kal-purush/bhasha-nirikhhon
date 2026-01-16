package study.Baekjoon.month9_2;

import java.io.*;
import java.util.StringTokenizer;

/**
 * 문제이름 : RGB거리
 * 링크 : https://www.acmicpc.net/problem/1149
 */

public class Baekjoon1149 {
    static int N;
    static int[][] cost;
    static int[][] dp;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        N = Integer.parseInt(br.readLine());
        cost = new int[N + 1][3];
        dp = new int[N + 1][3];
        for (int i = 1; i < N + 1; i++) {
            st = new StringTokenizer(br.readLine());
            cost[i][0] = Integer.parseInt(st.nextToken());
            cost[i][1] = Integer.parseInt(st.nextToken());
            cost[i][2] = Integer.parseInt(st.nextToken());
        }

        for (int i = 1; i < N + 1; i++) {
            for (int j = 0; j < 3; j++) {
                dp[i][j] = getMin(i - 1, j) + cost[i][j];   //이전 행중 (같은 j열제외)가장 작은 값 가져와서 비용과 더해줌
            }
        }

        int min = Integer.MAX_VALUE;
        for (int i = 0; i < 3; i++) {
            if (dp[N][i] < min)
                min = dp[N][i];
        }
        
            
        bw.write(min + "\n");
        bw.close();
    }
    
    //DP배열에서 같은 열을 제외한 나머지 열중에 가장 작은 값을 가져옴
    public static int getMin(int dpIdx, int color) {
        int color1 = -1, color2 = -1;
        switch (color) {
            case 0:
                color1 = 1;
                color2 = 2;
                break;
            case 1:
                color1 = 0;
                color2 = 2;
                break;
            case 2:
                color1 = 0;
                color2 = 1;
                break;
        }
        return Math.min(dp[dpIdx][color1], dp[dpIdx][color2]);
    }
}

/*


*/