package main.week6.BOJ11722;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * 가장 긴 감소하는 부분 수열, https://www.acmicpc.net/problem/11722, DP
 *
 * @author hazel
 */
public class BOJ11722 {

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        StringTokenizer st = new StringTokenizer(br.readLine());

        int[] arr = new int[n + 1];
        int[] dp = new int[n + 1];

        for (int i = 1; i <= n; i++) {
            arr[i] = Integer.parseInt(st.nextToken());
            dp[i] = 1; //1로 초기화!
        }

        //기저
        dp[1] = 1;
        for (int i = 1; i <= n; i++) { //arr[i]를 기준으로
            for (int j = 1; j < i; j++) {
                if (arr[i] < arr[j]) {
                    //점화식
                    dp[i] = Math.max(dp[i], dp[j] + 1); //max를 구하는 이유는 이전 원소들을 돌면서 최대값을 구해야하기때문에
                }
            }
        }

        Arrays.sort(dp);
        System.out.println(dp[n]);
    }

}