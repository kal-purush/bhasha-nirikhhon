package main.week6.BOJ15486;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author hazel
 */
public class BOJ15486 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());

        int[] time = new int[n + 1]; //상담 시간
        int[] price = new int[n + 1]; //상담 금액

        for (int i = 1; i <= n; i++) {
            String[] tp = br.readLine()
                            .split(" ");
            time[i] = Integer.parseInt(tp[0]);
            price[i] = Integer.parseInt(tp[1]);
        }

        int[] dp = new int[n + 2]; // i번째일까지 상담했을때는 i+1일에 수익을 얻음. 따라서 n+2

        for (int i = 1; i <= n; i++) { //i가 오늘 그리고 next가 돈받는날. 예를들어 1일부터 3일 일하면 4일에 돈 받는다.
            int next = i + time[i];
            if (next <= n + 1) { // n이 7일때 8인 다음날에 돈을 받으므로
                dp[next] = Math.max(dp[next], dp[i] + price[i]); //현재 갖고 있는 수익과 받을 수익 중 최대값
            }
            //5일의 상담이 진행중일때 6일의 상담은 불가능.
            //하지만 6일이 가질수있는 값은 5일까지의(전날까지의) 최대값이므로 최대값으로 업데이트
            dp[i + 1] = Math.max(dp[i], dp[i + 1]);
        }

        //System.out.println(Arrays.toString(dp));
        System.out.println(dp[n + 1]);
    }

}