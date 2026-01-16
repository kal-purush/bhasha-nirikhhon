package main.week6.BOJ1463;

import java.util.Scanner;

/**
 * 1로 만들기
 * https://www.acmicpc.net/problem/1463
 * DP
 *
 * @author hazel
 */
public class BOJ1463 {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        int n = scanner.nextInt();
        int dp[] = new int[1000001]; //1<= n <=10000000
        //기저 저장
        dp[2] = 1;
        dp[3] = 1;

        for (int i = 4; i <= n; i++) {
            //1을 뺄떄
            int cnt = dp[i - 1] + 1;
            //2로 나눌 수 있을때
            if (i % 2 == 0) {
                cnt = Math.min(cnt, dp[i / 2] + 1);
            }
            //3으로 나눌 수 있을때
            if (i % 3 == 0) {
                cnt = Math.min(cnt, dp[i / 3] + 1);
            }

            dp[i] = cnt;
        }
        System.out.println(dp[n]);
    }
}