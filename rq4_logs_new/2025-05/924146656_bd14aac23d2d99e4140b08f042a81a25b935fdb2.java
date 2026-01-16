package org.example.yhs3237.w14_longest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class baekjoon22862 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        StringTokenizer st = new StringTokenizer(br.readLine());

        int S = Integer.parseInt(st.nextToken()); // 수열의 길이
        int K = Integer.parseInt(st.nextToken()); // 삭제할 수 있는 최대 횟수

        int[] numbers = new int[S];
        st = new StringTokenizer(br.readLine());
        for (int i = 0; i < S; i++) {
            numbers[i] = Integer.parseInt(st.nextToken());
        }

        // start : 시작점, end : 끝점
        // oddCount : 홀수의 개수
        int start = 0, end = 0;
        int oddCount = 0;
        int maxLength = 0;

        // 시작점 끝점으로 범위를 조절해가면서 해당 범위 내에 홀수의 숫자가 몇개 있는지 체크
        // 홀수의 수가 K(삭제할 수 있는 최대 횟수)보다 클때는 홀수를 하나 삭제하고 start를 옮김
        // 홀수의 수가 K보다 작거나 같을 때는 범위 내 짝수의 개수를 세어 maxLength를 갱신
        while (end < S) {
            if (numbers[end] % 2 == 1) oddCount++;

            // K개까지 삭제할 수 있으므로 홀수의 수가 K개보다 큰 동안에 반복
            while (oddCount > K) {
                if (numbers[start] % 2 == 1) oddCount--;
                start++;
            }

            int evenCount = (end - start + 1) - oddCount;
            maxLength = Math.max(maxLength, evenCount);

            end++;
        }
        System.out.println(maxLength);
    }
}