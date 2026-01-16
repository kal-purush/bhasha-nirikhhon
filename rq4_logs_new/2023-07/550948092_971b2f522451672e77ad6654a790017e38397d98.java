package DoIt.BinarySearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

/*
    029 원하는 정수 찾기
    이진 탐색 복습
 */
public class Solution_2 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        StringTokenizer st = new StringTokenizer(br.readLine());
        int[] arr = new int[n+1];
        for(int i = 1; i <= n; i++){
            arr[i] = Integer.parseInt(st.nextToken());
        }
        int m = Integer.parseInt(br.readLine());
        int[] find = new int[m];
        st = new StringTokenizer(br.readLine());
        for(int i = 0; i < m; i++){
            find[i] = Integer.parseInt(st.nextToken());
        }
        Arrays.sort(arr);
        int start = 1, end = n;
        int[] answer = new int[m];
        for(int i = 0; i < m; i++) {
            while (start <= end) {
                int mid = (start + end) / 2;
                if (arr[mid] > find[i]){
                    end = mid - 1;
                } else if(arr[mid] < find[i]){
                    start = mid + 1;
                } else {
                    answer[i] = 1;
                    break;
                }
            }
            start = 1; end = n;
        }
        for(int i = 0; i < m; i++){
            System.out.println(answer[i]);
        }
    }
}