package study.Baekjoon.month9_2;

import java.io.*;
import java.util.StringTokenizer;

/**
 * 문제이름 : 연구소
 * 링크 : https://www.acmicpc.net/problem/14502
 */

public class Baekjoon14502 {
    static int N, M;
    static int[][] map;
    static boolean[][] visited;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        st = new StringTokenizer(br.readLine());
        N = Integer.parseInt(st.nextToken());
        M = Integer.parseInt(st.nextToken());

        map = new int[N][M];

    }

    public static int[][] copyArr() {
        int[][] temp = new int[N][M];
        for (int i = 0; i < N; i++)
            System.arraycopy(map[i], 0, temp[i], 0, M);

        return temp;
    }
    
    public static void combi(int r, int c, int cnt, int[][] point) {
        if (cnt == 3) {
            int[][] temp = copyArr();

        }
    }
    
    public static void bfs(int[][] m, int[][] point) {
        
    }
}

/*


*/