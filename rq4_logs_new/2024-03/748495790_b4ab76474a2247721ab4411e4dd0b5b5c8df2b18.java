package main.week6.BOJ9095;

import java.util.Scanner;

/**
 * 1,2,3 더하기
 * https://www.acmicpc.net/problem/9095
 * DP
 *
 * @author hazel
 */
public class BOJ9095 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int t = sc.nextInt();
        int[] dp;
        for (int i = 0; i < t; i++) {
            int n = sc.nextInt();
            dp = new int[11]; //n은 11보다 작다.

            //기저
            dp[1] = 1;
            dp[2] = 2;
            dp[3] = 4;

            for (int j = 4; j <= n; j++) {
                dp[j] = dp[j - 1] + dp[j - 2] + dp[j - 3];
            }
            System.out.println(dp[n]);
        }
    }
}