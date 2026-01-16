package org.example.rt8632.w14_LongestSubsequenceLarge;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class boj_22862 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        // 입력
        StringTokenizer st = new StringTokenizer(br.readLine());
        int N = Integer.parseInt(st.nextToken()); // 수열 길이
        int K = Integer.parseInt(st.nextToken()); // 제거 가능한 홀수 개수

        int[] arr = new int[N];
        st = new StringTokenizer(br.readLine());
        for(int i = 0; i < N; i++) {
            arr[i] = Integer.parseInt(st.nextToken());
        }

        // 투 포인터
        int left = 0, right = 0;
        int oddCount = 0;
        int maxLength = 0;

        while (right < N) {
            if (arr[right] % 2 == 0) {
                // 짝수일 때
                right++;
            } else {
                // 홀수일 때
                if (oddCount < K) {
                    oddCount++;
                    right++;
                } else {
                    // 홀수 개수가 K개 넘으면 left를 옮기면서 제거
                    if (arr[left] % 2 == 1) {
                        oddCount--;
                    }
                    left++;
                }
            }
            maxLength = Math.max(maxLength, right - left - oddCount);
        }

        System.out.println(maxLength);
    }
}