package DoIt.DynamicProgramming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/*
    089 연속합
    [DP 문제]
    왼쪽, 오른쪽 각각에서 최대합 구하기 -> 자기 위치 값과 이전 위치값 + 현위치 값 중 큰 것 고르며 업데이트
    한 위치 값을 삭제할 수 있음 -> 현 위치를 제외 - (현위치-1) 위치의 왼쪽 최대합 값 + (현위치+1) 위치의 오른쪽 최대값 
 */
public class Solution8 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st  = new StringTokenizer(br.readLine());
        int n = Integer.parseInt(st.nextToken());
        int[] arr = new int[n];
        st = new StringTokenizer(br.readLine());
        for(int i = 0; i < n; i++){
            arr[i] = Integer.parseInt(st.nextToken());
        }
        int[] L = new int[n];
        int[] R = new int[n];
        L[0] =  arr[0];
        for(int i = 1; i < n; i++){
            L[i] = Math.max(arr[i], L[i-1] + arr[i]);
        }
        R[n-1] = arr[n-1];
        for(int i = n-2; i >= 0; i--){
            R[i] = Math.max(arr[i], R[i+1] + arr[i]);
        }
        int max = L[0];
        for(int i = 1; i < n; i++){
            max = Math.max(max, L[i]);
        }

        for(int i = 1; i < n-1; i++){
            if(i > 0 && i < n-1){
                max = Math.max(max, L[i-1] + R[i+1]);
            }
        }
        System.out.println(max);
    }
}