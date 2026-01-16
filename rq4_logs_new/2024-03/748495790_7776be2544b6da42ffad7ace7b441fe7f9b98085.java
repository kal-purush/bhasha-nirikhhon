package main.week6.BOJ11726;

import java.util.Scanner;

/**
 * 2xn 타일링
 * https://www.acmicpc.net/problem/11726
 * DP
 *
 * @author hazel
 */
public class BOJ11726 {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        int[] dp = new int[1001];

        //기저
        dp[1] = 1;
        dp[2] = 2;
        for (int i = 3; i <= n; i++) {
            //점화식
            dp[i] = (dp[i - 2] + dp[i - 1]) % 10007;
        }

        System.out.println(dp[n]);

    }
}