package PHS00;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

// 기타 레슨
public class BOJ_2343 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = null;
        st = new StringTokenizer(br.readLine());

        int N = Integer.parseInt(st.nextToken());
        int M = Integer.parseInt(st.nextToken());

        st = new StringTokenizer(br.readLine());
        int[] images = new int[N];
        int start = 0, end = 0;
        for(int i = 0; i < N; i++){
            images[i] = Integer.parseInt(st.nextToken());
            end += images[i];
            start = Math.max(start, images[i]);
        }
        while(start < end){
            int mid = (start + end) / 2;
            int sum = 0;
            int cnt = 0;
            for(int i = 0; i < N; i++){
                if(sum + images[i]> mid){
                    // 강의 시간이 제한 시간 보다 큰 경우 블루레이 수 증가
                    cnt++;
                    sum = 0;
                }
                sum += images[i];
            }
            if(sum != 0){
                cnt++;
            }
            if(cnt > M){
                // 블루레이 수가 제안 수 보다 많으면
                start = mid+1;
            }else{
                end = mid;
            }
        }
        System.out.println(end);
    }
}
