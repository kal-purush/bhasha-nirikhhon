package study.Baekjoon.month9_2;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

/**
 * 문제이름 : 말이 되고픈 원숭이
 * 링크 : https://www.acmicpc.net/problem/1600
 */

public class Baekjoon1600 {
    static int[][] hDir = { { 1, 2 }, { 2, 1 }, { 1, -2 }, { 2, -1 }, { -1, 2 }, { -2, 1 }, { -1, -2 }, { -2, -1 } };
    static int[][] dir = { { 0, 1 }, { 1, 0 }, { 0, -1 }, { -1, 0 } }; //우 하 좌 상
    static int[][] map;
    static int K;
    static int W, H;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        K = Integer.parseInt(br.readLine());
        st = new StringTokenizer(br.readLine());
        W = Integer.parseInt(st.nextToken()); //열
        H = Integer.parseInt(st.nextToken()); //행
        map = new int[H][W];

        for (int i = 0; i < H; i++) {
            st = new StringTokenizer(br.readLine());
            for (int j = 0; j < W; j++) {
                map[i][j] = Integer.parseInt(st.nextToken());
            }
        }

        int result = bfs(0, 0);
        bw.write(result + "\n");
        bw.close();
    }

    public static int bfs(int r, int c) {
        boolean[][][] v = new boolean[H][W][K+1];
        Queue<Point> q = new LinkedList<>();
        q.offer(new Point(r, c, 0, K));
        v[r][c][0] = true;
        boolean isEnd = false;
        Point p = null;
        while (!q.isEmpty()) {
            p = q.poll();

            if (p.r == H - 1 && p.c == W - 1) {
                isEnd = true;
                break;
            }

            for (int i = 0; i < dir.length; i++) { //한칸씩 이동하는 경우
                int dr = p.r + dir[i][0];
                int dc = p.c + dir[i][1];

                if (dr < 0 || dr >= H || dc < 0 || dc >= W || map[dr][dc] == 1 || v[dr][dc][p.k])
                    continue;

                q.offer(new Point(dr, dc, p.moveCnt + 1, p.k));
                v[dr][dc][p.k] = true;
            }

            if (p.k == 0) //말처럼 이동하는 횟수 모두 소진
                continue;

            for (int i = 0; i < hDir.length; i++) { //말처럼 이동하는 경우
                int dr = p.r + hDir[i][0];
                int dc = p.c + hDir[i][1];

                if (dr < 0 || dr >= H || dc < 0 || dc >= W || map[dr][dc] == 1 || v[dr][dc][p.k - 1])
                    continue;

                q.offer(new Point(dr, dc, p.moveCnt + 1, p.k - 1));
                v[dr][dc][p.k - 1] = true;
            }

        }
        
        if (isEnd) 
            return p.moveCnt;
        else
            return -1;
    }
}

class Point {
    int r;
    int c;
    int moveCnt;
    int k;

    public Point(int r, int c, int moveCnt, int k) {
        this.r = r;
        this.c = c;
        this.moveCnt = moveCnt;
        this.k = k;
    }
}

/*

-2-2 -2-1 -20  -21  -22
-1-2 -1-1 -10  -11  -12
0-2  0-1   00   01   02
1-2  1-1   10   11   12
2-2  2-1   20   21   22

{-2,-1},{-2,1},{-1,-2},{-1,2},{1,-2},{1,2},{2,-1},{2,1}
{1,2},{2,1},{1,-2},{2,-1},{-1,2},{-2,1},{-1,-2},{-2,-1} //도착지점으로 갈 때
{-1,-2},{-2,-1},{-2,1},{1,-2},{-1,2},{2,-1},{1,2},{2,1}

알고리즘
1. 한칸씩 이동해서 도착지점까지 도착
    1-2. 만약 도착지점에 도착할 수 없으면 
*/