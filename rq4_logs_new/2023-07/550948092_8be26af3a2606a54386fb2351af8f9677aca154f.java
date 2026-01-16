package DoIt.Floyd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

/*
    063 케빈 베이컨의 6단계 법칙
 */
public class Solution3_2 {
    static public int distance[][];
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        int n  = Integer.parseInt(st.nextToken());
        int m = Integer.parseInt(st.nextToken());
        distance = new int[n+1][n+1];
        for(int i = 1; i <= n; i++){
            for(int j = 1; j <= n; j++) {
                if(i==j) distance[i][j] = 0;
                else distance[i][j] = 10000001;
            }
        }
        for(int i = 0; i < m; i++){
            st = new StringTokenizer(br.readLine());
            int s = Integer.parseInt(st.nextToken());
            int e = Integer.parseInt(st.nextToken());
            distance[s][e] = 1;
            distance[e][s] = 1;
        }
        for(int k = 1; k <= n; k++){
            for(int i = 1; i <= n; i++){
                for(int j = 1; j <= n; j++){
                    if(distance[i][j] > distance[i][k]+distance[k][j])
                        distance[i][j] = distance[i][k]+distance[k][j];
                }
            }
        }
        int min = Integer.MAX_VALUE;
        int answer = -1;
        for(int i = 1; i <= n; i++){
            int temp = 0;
            for(int j = 1; j <= n; j++){
                temp += distance[i][j];
            }
            if(min > temp){
                min = temp;
                answer = i;
            }
        }
        System.out.println(answer);
    }
}