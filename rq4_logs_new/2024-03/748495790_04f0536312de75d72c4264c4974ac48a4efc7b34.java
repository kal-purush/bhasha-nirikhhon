package main.week5.BOJ6236;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 용돈관리
 * https://www.acmicpc.net/problem/6236
 * 이분탐색
 *
 * @author hazel
 */
public class BOJ6236 {
    static int result = 0;
    static int n, m;
    static int[] arr;

    /*
        남은 금액이 사용해야할 금액보다 많다면 사용, 만약 부족하다면 다시 통장에 돈을 넣고 다시 인출
        이 인출 횟수가 총 m번이 되야한다.
        단 m번을 맞추기 위해서 남은 금액이 충분하더라도 통장에 넣고 다시 인출을 할 수 있다.
     */
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] nm = br.readLine().split(" ");

        n = Integer.parseInt(nm[0]);
        m = Integer.parseInt(nm[1]);
        arr = new int[n];

        //최대값
        int maxValue = Integer.MIN_VALUE;
        int sum = 0;
        for (int i = 0; i < n; i++) {
            arr[i] = Integer.parseInt(br.readLine());
            sum += arr[i];
            maxValue = Math.max(arr[i], maxValue);
        }

        int left = maxValue;
        int right = sum; //이진탐색 범위의 최대값은 모든 이용할 금액의 합
        while (left <= right) {

            int mid = (left + right) / 2;
            //금액 K로 인출 가능한 횟수 구하기
            int cnt = getWithdrawalCount(mid);
            if (m >= cnt) {//예를 들어 cnt가 5이고, m이 7일때 인출 횟수를 늘리려면 금액을 줄여야함.
                right = mid - 1;
                result = mid;
            } else {
                //cnt가 m보다 클떄 인출 횟수를 줄이려면 금액을 늘려한다.
                left = mid + 1;
            }
        }
        System.out.println(result);
    }

    //인출해야하는 횟수 구하기
    public static int getWithdrawalCount(int mid) {

        int cnt = 1;
        //money 는 내가 인출해서 가지고 있는 돈
        int money = mid; //mid는 내가 출금한 금액 k를 말함

        for (int tmp : arr) {
            money = money - tmp;

            //돈이 모자를때 현금을 다시 인출하기
            if (money < 0) {
                ++cnt; // 전위증감연산자!
                money = mid - tmp;
            }
        }
        return cnt;
    }

}