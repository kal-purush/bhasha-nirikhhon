package JiEung2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class BOJ1654 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());

        int N = Integer.parseInt(st.nextToken());
        int K = Integer.parseInt(st.nextToken());
        long left = 0, right = 0;
        int[] arr = new int[N];
        for (int i = 0; i < N; i++) {
            arr[i] = Integer.parseInt(br.readLine());
            right = Math.max(right, arr[i]);
        }
        right++;
        long mid = 0;
        while (left<right) {
            mid = (left + right) / 2;

            long cnt = check(mid, arr);

            if (cnt < K) {
                right = mid;
            } else{
                left = mid+1;
            }
        }
        System.out.println(left -1);
    }

    public static long check(long mid, int[] arr) {
        long cnt = 0;
        for (int i = 0; i < arr.length; i++) {
            cnt += arr[i] / mid;
        }
        return cnt;
    }
}