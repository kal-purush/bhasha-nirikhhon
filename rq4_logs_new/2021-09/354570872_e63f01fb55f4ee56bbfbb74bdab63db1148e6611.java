package study.SWEA.etc.SWEA4615;

import java.io.*;
import java.util.StringTokenizer;

/**
 * 문제이름 : 재미있는 오셀로 게임
 * 링크 : https://swexpertacademy.com/main/code/problem/problemDetail.do?contestProbId=AWQmA4uK8ygDFAXj&categoryId=AWQmA4uK8ygDFAXj&categoryType=CODE&problemTitle=%EC%98%A4%EC%85%80%EB%A1%9C&orderBy=FIRST_REG_DATETIME&selectCodeLang=ALL&select-1=&pageSize=10&pageIndex=1
 */

public class SWEA4615 {
    static int N, M;
    static int[][] board;
    static int[][] dir = { { -1, 0 }, { 1, 0 }, { 0, -1 }, { 0, 1 }, { -1, -1 }, { -1, 1 }, { 1, -1 }, { 1, 1 }};
    static int W, B;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        int tc = Integer.parseInt(br.readLine());
        for (int t = 1; t <= tc; t++) {
            st = new StringTokenizer(br.readLine());
            N = Integer.parseInt(st.nextToken());
            M = Integer.parseInt(st.nextToken());

            board = new int[N][N];
            board[N / 2 - 1][N / 2 - 1] = 2;
            board[N / 2 - 1][N / 2] = 1;
            board[N / 2][N / 2 - 1] = 1;
            board[N / 2][N / 2] = 2;

            for (int i = 0; i < M; i++) {
                st = new StringTokenizer(br.readLine());
                int r = Integer.parseInt(st.nextToken()) - 1;
                int c = Integer.parseInt(st.nextToken()) - 1;
                int color = Integer.parseInt(st.nextToken());
                play(c, r, color);
            }

            W = 0;
            B = 0;
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < N; j++) {
                    if (board[i][j] == 1)
                        B++;
                    else if (board[i][j] == 2)
                        W++;
                }
            }

            bw.write("#" + t + " " + B + " " + W + "\n");
        }
        bw.close();
    }
    
    public static void play(int r, int c, int color) {
        if (color == 1) {   // 흑돌 - 1
            for (int i = 0; i < dir.length; i++) {
                int dr = r + dir[i][0];
                int dc = c + dir[i][1];

                if (dr < 0 || dr >= N || dc < 0 || dc >= N || board[dr][dc] != 2) 
                    continue;
                
                dr += dir[i][0];
                dc += dir[i][1];

                boolean change = false;
                while (dr >= 0 && dr < N && dc >= 0 && dc < N) {
                    if (board[dr][dc] == 0) 
                        break;

                    if (board[dr][dc] == 1) {
                        change = true;
                        break;
                    }
                    dr += dir[i][0];
                    dc += dir[i][1];
                }

                if (change) {
                    int R = r;
                    int C = c;
                    board[R][C] = 1;
                    while (R != dr || C != dc) {
                        R += dir[i][0];
                        C += dir[i][1];
                        board[R][C] = 1;
                    }
                }
            }
        } else { // 백돌 - 2
            for (int i = 0; i < dir.length; i++) {
                int dr = r + dir[i][0];
                int dc = c + dir[i][1];

                if (dr < 0 || dr >= N || dc < 0 || dc >= N || board[dr][dc] != 1) 
                    continue;
                
                dr += dir[i][0];
                dc += dir[i][1];

                boolean change = false;
                while (dr >= 0 && dr < N && dc >= 0 && dc < N) {
                    if (board[dr][dc] == 0) 
                        break;
                    
                    if (board[dr][dc] == 2) {
                        change = true;
                        break;
                    }
                    
                    dr += dir[i][0];
                    dc += dir[i][1];
                }

                if (change) {
                    int R = r;
                    int C = c;
                    board[R][C] = 2;
                    while (R != dr || C != dc) {
                        R += dir[i][0];
                        C += dir[i][1];
                        board[R][C] = 2;
                    }
                }
            }
        }
    }
}

/*


*/