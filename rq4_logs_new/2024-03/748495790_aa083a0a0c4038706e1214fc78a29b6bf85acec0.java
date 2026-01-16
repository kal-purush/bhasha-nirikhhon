package main.week5.BOJ1300;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * k번째 수
 * https://www.acmicpc.net/problem/1300
 * 이진 탐색
 *
 * @author hazel
 */
public class BOJ1300 {
    public static void main(String[] args) throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int k = Integer.parseInt(br.readLine());

        //todo 왜 최대값이 7일까?
        //인덱스가 아닌 x를 기준으로 생각해야함
        long low = 1;
        long high = k; //왜 n*n가 최대값이 아니고 n일까? 아직도 매우매우 이해불가.....
        long answer = 0;

        while (low <= high) {
            long mid = (low + high) / 2; //중간 값
            long cnt = 0;
            for (int i = 1; i <= n; i++) {
                cnt += Math.min(mid / i, n);
            }

            if (cnt < k) {
                low = mid + 1;
            } else {
                answer = mid;
                high = mid - 1;
            }
        }
        System.out.print(answer);
    }
}