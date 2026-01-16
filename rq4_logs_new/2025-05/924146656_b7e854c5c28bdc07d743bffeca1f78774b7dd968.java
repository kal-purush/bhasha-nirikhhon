package org.example.rt8632.w12_Tettromino;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class boj_14500 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        int N = Integer.parseInt(st.nextToken());
        int M = Integer.parseInt(st.nextToken());

        int[][] map = new int[N][M];

        //입력
        for(int row = 0; row < N; row++) {
            st = new StringTokenizer(br.readLine());
            for(int col = 0; col < M; col++) {
                map[row][col] = Integer.parseInt(st.nextToken());
            }
        }
        int ans = 0;
        int result = 0;
        //1번 테트로미노 검사
        for(int row = 0; row < N; row++) {
            for(int col = 0; col < M; col++) {
                result = 0;
                for(int k=0;k<4;k++){
                    if(col+k > M-1)
                        continue;
                    result += map[row][col+k];
                }
                ans = Math.max(ans, result);
            }
        }
        System.out.println("1" + " " + ans);

        //2번 테트로미노
        for(int row = 0; row < N; row++) {
            for(int col = 0; col < M; col++) {
                result = 0;
                for(int i=0;i<2;i++){
                    for(int j=0;j<2;j++){
                        if(row+i > N-1 || col+j > M-1)
                            continue;
                        result += map[row+i][col+j];
                    }
                }
                ans = Math.max(ans, result);
            }
        }

        System.out.println("2 " + ans);
        //3번 테트로미노
        for(int row = 0; row < N; row++) {
            for(int col = 0; col < M; col++) {
                result = 0;
                for(int i=0;i<3;i++){
                    if(row + i > N-1)
                        continue;
                    result += map[row+i][col];
                }
                if(row + 2 > N -1 || col+1 > M-1)
                    continue;
                result += map[row+2][col+1];
                ans = Math.max(ans, result);
            }
        }
        System.out.println("3 " + ans);
        //4번 테트로미노
        for(int row = 0; row < N; row++) {
            for(int col = 0; col < M; col++) {
                result = 0;
                for(int i=0;i<2;i++){
                    if(row + i> N-1)
                        continue;
                    result += map[row+i][col];
                }
                for(int j=0;j<2;j++){
                    if(row+j+1 > N-1|| col+1 > M-1)
                        continue;
                    result += map[row+1+j][col+1];
                }
                ans = Math.max(ans, result);
            }
        }
        System.out.println("4 " + ans);
        //5번 테트로미노
        for(int row = 0; row < N; row++) {
            for(int col = 0; col < M; col++) {
                result = 0;
                for(int i=0;i<3;i++){
                    if(col + i > M-1)
                        continue;
                    result += map[row][col+i];
                }
                if(row + 1 > N-1|| col +1 > M-1)
                    continue;
                result += map[row+1][col+1];
                ans = Math.max(ans, result);
            }
        }

        System.out.println(ans);
    }
}
