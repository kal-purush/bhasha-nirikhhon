package BaekJoon.Greedy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.StringTokenizer;

/*
    2212 센서
    그리디, 일직선, 집합 분류와 엮어 생각하기
 */
public class Solution1 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int k = Integer.parseInt(br.readLine());
        StringTokenizer st = new StringTokenizer(br.readLine());
        int[] arr = new int[n];
        for(int i = 0; i < n; i++){
            arr[i] = Integer.parseInt(st.nextToken());
        }
        Arrays.sort(arr);
        Integer[] gap = new Integer[n-1];
        for(int i = 0; i < n-1; i++){
            gap[i] = arr[i+1] - arr[i];
        }
        int answer = 0;
        Arrays.sort(gap, Collections.reverseOrder());
        for(int i = k-1; i < n-1; i++){
            answer += gap[i];
        }
        System.out.println(answer);
    }
}