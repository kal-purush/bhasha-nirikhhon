package BaekJoon.String;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

/*
    9081 단어맞추기
    문자열
 */
public class Solution {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());

        for(int i = 0; i < n; i++){
            System.out.println(solution(br.readLine()));
        }
    }

    static String solution(String s){
        StringBuilder sb = new StringBuilder();
        int n = s.length();

        char[] arr = s.toCharArray();
        int idx1 = -1, idx2 = 0;

        for(int i = n-1; i > 0; i--){
            if(arr[i-1] < arr[i]){
                idx1 = i-1;
                break;
            }
        }
        if(idx1 == -1) return s;

        for(int i = n-1; i >= 0; i--){
            if(arr[idx1] < arr[i]){
                idx2 = i;
                break;
            }
        }

        char tmp = arr[idx1];
        arr[idx1] = arr[idx2];
        arr[idx2] = tmp;

        Arrays.sort(arr, idx1+1, n);
        for(char c: arr) sb.append(c);

        return sb.toString();
    }
}