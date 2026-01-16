package PHS00;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

// 랜선 자르기
public class BOJ_1654 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = null;
        st = new StringTokenizer(br.readLine());

        int N = Integer.parseInt(st.nextToken());
        int K = Integer.parseInt(st.nextToken());
        long[] ropes = new long[N];
        long start = 0, end = 0;
        for(int i = 0; i < N; i++){
            ropes[i] = Integer.parseInt(br.readLine());
            end = Math.max(end, ropes[i]);
        }

        end++;
        while (start < end) {
            long cnt = 0;
            long mid = (start + end) / 2;
            for (int i = 0; i < N; i++) {
                cnt += (ropes[i] / mid);
            }
            if(cnt < K) {
                end = mid;
            }
            else {
                start = mid + 1;
            }
        }
        System.out.println(start - 1);
    }
}