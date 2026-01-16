package JiEung2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class BOJ2343 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());

        int N = Integer.parseInt(st.nextToken());
        int M = Integer.parseInt(st.nextToken());

        st = new StringTokenizer(br.readLine());
        int[] arr = new int[N];
        int left = 0, right = 0;

        for(int i=0 ; i<N; i++){
            arr[i] = Integer.parseInt(st.nextToken());
            right += arr[i];
            left = Math.max(left, arr[i]);
        }

        while(left <= right){
            int mid = (left + right) / 2;
            int cnt = check(N, arr, mid);
            if (cnt > M) {
                left = mid + 1;
            } else{
                right = mid - 1;
            }
        }
        System.out.println(left);
    }

    public static int check(int n, int[] arr, int mid) {
        int sum = 0;
        int cnt = 0;
        for(int i=0; i<n; i++){
            if (sum + arr[i] > mid) {
                sum = 0;
                cnt++;
            }
            sum+= arr[i];
        }
        if (sum != 0) cnt++;
        return cnt;
    }
}