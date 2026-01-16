package DoIt.DynamicProgramming;

import java.io.IOException;
import java.util.Scanner;

/*
    094 행렬 곱 연산 횟수의 최솟값 구하기
 */
public class Solution6 {
    static int n;
    static int r, c;
    static int[][] map;
    static int[][] dp;
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        n = sc.nextInt();
        map = new int[n][2];
        dp = new int[n][n];
        for(int i = 0; i < n; i++){
            map[i][0] = sc.nextInt();
            map[i][1] = sc.nextInt();
        }
        for(int i = 0; i < n; i++){
            for(int j = 0; j < n; j++){
                if(i == j)
                    dp[i][j] = 0;
                else
                    dp[i][j] = Integer.MAX_VALUE;
            }
        }
        for(int c = 1; c < n; c++) {
            for (int i = 0; i + c < n; i++)
                calcul(i, i+c);
        }
        System.out.println(dp[0][n-1]);
    }
    private static void calcul(int start, int end){
        for(int i = start; i < end; i++){
            int cost = dp[start][i] + dp[i+1][end] + map[start][0] * map[i][i] * map[end][1];
            dp[start][end] = Math.min(dp[start][end], cost);
        }
    }
}