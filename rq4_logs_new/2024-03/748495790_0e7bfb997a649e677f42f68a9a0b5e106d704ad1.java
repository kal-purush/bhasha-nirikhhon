package main.week5.BOJ1654;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * 랜선 자르기
 * 이진탐색
 *
 * @author hazel
 */
public class BOJ1654 {
    public static void main(String[] args) throws IOException {  //Checked Exceptions - 반드시 처리해야하는 예외
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] nk = br.readLine().split(" ");
        int k = Integer.parseInt(nk[0]); //가지고 있는 랜선
        int n = Integer.parseInt(nk[1]); //필요한 랜선 개수
        int[] arr = new int[n];
        for (int i = 0; i < k; i++) {
            arr[i] = Integer.parseInt(br.readLine());
        }

        int maxValue = Integer.MIN_VALUE; //랜선의 최대값이 2^31 -1
        //최대값찾기
        for (int i = 0; i < k; i++) {
            maxValue = Math.max(arr[i], maxValue);
        }
        long low = 1; //랜선의 길이는 자연수이므로 1
        long high = maxValue;

        while (low <= high) {
            long mid = (low + high) / 2;  //high의 최대값이 int의 최대값이므로 mid는 long으로 설정
            int cnt = 0;
            for (int i = 0; i < k; i++) {
                cnt += arr[i] / mid;
            }

            //n이 cnt보다 더 크면 mid 보다 이전 범위 검색
            if (cnt < n) {
                high = mid - 1;
            } else { //cnt가 n보다 같거나 클때
                low = mid + 1;
            }

        }
        System.out.println(high);
    }
}