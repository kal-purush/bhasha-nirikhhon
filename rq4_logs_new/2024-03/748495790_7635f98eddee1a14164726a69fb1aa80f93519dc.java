package main.week6.BOJ2579;

import java.util.Scanner;

/**
 * 계단 오르기
 * https://www.acmicpc.net/problem/2579
 * DP
 *
 * @author hazel
 */
public class BOJ2579 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int n = sc.nextInt();
        int[] stairs = new int[n + 1]; //계단 점수 저장
        int[] dp = new int[n + 1]; //최대값 저장

        for (int i = 1; i <= n; i++) {
            stairs[i] = sc.nextInt();
        }

        dp[1] = stairs[1];

        //dp[2] = stairs[1] + stairs[2] 으로 만들수있지만
        //n이 1인경우도 존재하기 때문에 예외처리를 해줘야함.
        if (n >= 2) {
            dp[2] = stairs[1] + stairs[2];
        }
        for (int i = 3; i <= n; i++) {
            //예를 들어 dp[4]의 최대값은 dp[2]과 dp[3]의 최대값이다.따라서 dp[3]은 dp[1] + stairs[3]
            dp[i] = Math.max(dp[i - 2], dp[i - 3] + stairs[i - 1]) + stairs[i];
        }

        System.out.println(dp[n]);

    }
}