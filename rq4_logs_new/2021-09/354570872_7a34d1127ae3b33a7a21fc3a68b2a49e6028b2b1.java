package study.Baekjoon.month9_3;

import java.io.*;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

/**
 * 문제이름 : 미세먼지 안녕!
 * 링크 : https://www.acmicpc.net/problem/
 */

public class Baekjoon17144 {
    static int R, C, T;
    static int[][] map;
    static int[] topCleaner;    //위쪽 공기청정기 좌표
    static int[] bottomCleaner; //아래쪽 공기청정기 좌표
    static int[][] dir = { { -1, 0 }, { 1, 0 }, { 0, -1 }, { 0, 1 } };
    static int[][] rClockDir = { { 0, 1 }, { -1, 0 }, { 0, -1 }, { 1, 0 } }; //우 상 좌 하
    static int[][] clockDir = { { 0, 1 }, { 1, 0 }, { 0, -1 }, { -1, 0 } };   //우 하 좌 상
    
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;

        st = new StringTokenizer(br.readLine());
        R = Integer.parseInt(st.nextToken());
        C = Integer.parseInt(st.nextToken());
        T = Integer.parseInt(st.nextToken());

        map = new int[R][C];
        topCleaner = new int[] { -1, -1 };
        bottomCleaner = new int[] { -1, -1 };
        for (int i = 0; i < R; i++) {
            st = new StringTokenizer(br.readLine());
            for (int j = 0; j < C; j++) {
                map[i][j] = Integer.parseInt(st.nextToken());
                if (map[i][j] == -1) {
                    if (topCleaner[0] == -1) {
                        topCleaner[0] = i;
                        topCleaner[1] = j;
                    } else {
                        bottomCleaner[0] = i;
                        bottomCleaner[1] = j;
                    }
                }
            }
        }

        for (int i = 0; i < T; i++) {
            spread();
            cleaning();
        }

        int result = sumDust();
        bw.write(result + "\n");
        bw.close();
    }
    
    public static void spread() {
        Queue<int[]> q = new LinkedList<>();
        for (int i = 0; i < R; i++) {
            for (int j = 0; j < C; j++) {
                if (map[i][j] >= 5)
                    q.offer(new int[] { i, j, map[i][j] });
            }
        }

        while (!q.isEmpty()) {
            int[] t = q.poll();
            int r = t[0];
            int c = t[1];
            int dust = t[2];
            int spreadDust = dust / 5;

            for (int i = 0; i < dir.length; i++) {
                int dr = r + dir[i][0];
                int dc = c + dir[i][1];

                if (dr < 0 || dr >= R || dc < 0 || dc >= C || map[dr][dc] == -1)
                    continue;

                map[dr][dc] += spreadDust;
                map[r][c] -= spreadDust;
            }
        }

    }
    
    public static void cleaning() {
        int topR = topCleaner[0];
        int topC = topCleaner[1];

        int botR = bottomCleaner[0];
        int botC = bottomCleaner[1];

        int topHeight = topR - 1; //위쪽 공기청정기의 행 길이
        int botHeight = R - botR - 1; //아래쪽 공기청정기의 행 길이

        int d = 0;
        int prevValue = 0;
        for (int i = 0; i < 2 * C + 2 * topHeight; i++) {
            topR += rClockDir[d][0];
            topC += rClockDir[d][1];

            if (topR < 0 || topR >= R || topC < 0 || topC >= C) {
                topR -= rClockDir[d][0];
                topC -= rClockDir[d][1];
                d = (d + 1) % 4;
                i--;
                continue;
            }

            if (map[topR][topC] == -1)
                break;

            int temp = map[topR][topC];
            map[topR][topC] = prevValue;
            prevValue = temp;
        }

        d = 0;
        prevValue = 0;
        for (int i = 0; i < 2 * C + 2 * botHeight; i++) {
            botR += clockDir[d][0];
            botC += clockDir[d][1];

            if (botR < 0 || botR >= R || botC < 0 || botC >= C) {
                botR -= clockDir[d][0];
                botC -= clockDir[d][1];
                d = (d + 1) % 4;
                i--;
                continue;
            }

            if (map[botR][botC] == -1)
                break;

            int temp = map[botR][botC];
            map[botR][botC] = prevValue;
            prevValue = temp;
        }
    }
    
    public static int sumDust() {
        int sum = 0;
        for (int i = 0; i < R; i++) {
            for (int j = 0; j < C; j++) {
                if (map[i][j] == -1)
                    continue;
                sum += map[i][j];
            }
        }
        
        return sum;
    }
}

/*


*/