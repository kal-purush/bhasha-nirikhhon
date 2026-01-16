//package study.SWEA.month9_2.SWEA1767;

import java.io.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * 문제이름 : 프로세서 연결하기
 * 링크 : https://swexpertacademy.com/main/code/problem/problemDetail.do?contestProbId=AV4suNtaXFEDFAUf&categoryId=AV4suNtaXFEDFAUf&categoryType=CODE&problemTitle=%ED%94%84%EB%A1%9C%EC%84%B8%EC%84%9C&orderBy=FIRST_REG_DATETIME&selectCodeLang=ALL&select-1=&pageSize=10&pageIndex=1
 */

public class SWEA1767 {
    static int N;
    static int[][] map;
    static ArrayList<int[]> core;
    static int[][] dir = {{-1,0},{1,0},{0,-1},{0,1}};
    static int maxCoreCnt;
    static int minLength;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        StringTokenizer st;
        
        int tc = Integer.parseInt(br.readLine());
        for (int t = 1; t <= tc; t++) {
            N = Integer.parseInt(br.readLine());
            map = new int[N][N];
            core = new ArrayList<>();
            maxCoreCnt = 0;
            minLength = Integer.MAX_VALUE;
            for (int i = 0; i < N; i++) {
                st = new StringTokenizer(br.readLine());
                for (int j = 0; j < N; j++) {
                    map[i][j] = Integer.parseInt(st.nextToken());
                    if (map[i][j] == 1 && !((i == 0 || i == N - 1) || (j == 0 || j == N - 1))) 
                        core.add(new int[]{i,j});
                }
            }

            dfs(0, 0, 0);

            bw.write("#"+t+" "+minLength+"\n");
        }
        bw.close();
    }

    public static void dfs(int coreCnt, int coreIdx, int len) {
        if (coreIdx == core.size()) {
            if (maxCoreCnt < coreCnt) {
                maxCoreCnt = coreCnt;
                minLength = len;
            } else if (maxCoreCnt == coreCnt) 
                minLength = Math.min(minLength, len);
            
            return;
        }

        int[] p = core.get(coreIdx);
        int r = p[0];
        int c = p[1];

        for (int i = 0; i < dir.length; i++) {
            int cnt = countWire(r, c, i);

            drawWire(r, c, i, cnt, 1);

            if (cnt == 0) //연결할 전선 없음
                dfs(coreCnt, coreIdx+1, len);
            else {
                dfs(coreCnt+1, coreIdx+1, len+cnt);

                drawWire(r, c, i, cnt, 0);  //전선 그린거 다시 지움
            }

        }
    }

    public static int countWire(int r, int c, int d) {
        int dr = r;
        int dc = c;
        int cnt = 0;
        while (true) {
            dr += dir[d][0];
            dc += dir[d][1];

            if (dr < 0 || dr >= N || dc < 0 || dc >= N) 
                    break;
                
                if (map[dr][dc] == 1) {
                    cnt = 0;
                    break;
                }

                cnt++;
        }
        return cnt;
    }

    public static void drawWire(int r, int c, int d, int len, int data) {
        int dr = r;
        int dc = c;
        for (int i = 0; i < len; i++) {
            dr += dir[d][0];
            dc += dir[d][1];

            map[dr][dc] = data;
        }
    }
}

/*
  1
  2
      3
    
45 6
 7

1 - 0, -1, 2, 4     1
2 - -1, 5, 2, 4     3
3 - 2, 4, 5, 1      4
4 - 4, 2, 0, -1     1
5 - 4, -1, -1, -1   1
6 - 4, 2, -1, 3     3
7 - -1, 1, 1, 5     3


1, 4, 5 번 선만듬 - 무조건
2 - -1, -1, -1, 4   1
3 - 2, 4, -1, 1     3
6 - 4, 2, -1, 3     3
7 - -1, 1, 1, 5     3

2 번 선 만듬
3 - -1, 4, -1, 1    2
6 - -1, 2, -1, 3    3
7 - -1, 1, 1, 5     3


3
7
0 0 1 0 0 0 0
0 0 1 0 0 0 0
0 0 0 0 0 1 0
0 0 0 0 0 0 0
1 1 0 1 0 0 0
0 1 0 0 0 0 0
0 0 0 0 0 0 0
9
0 0 0 0 0 0 0 0 0
0 0 1 0 0 0 0 0 1
1 0 0 0 0 0 0 0 0
0 0 0 1 0 0 0 0 0
0 1 0 0 0 0 0 0 0
0 0 0 0 0 0 1 0 0
0 0 0 1 0 0 0 0 0
0 0 0 0 0 0 0 1 0
0 0 0 0 0 0 0 0 1
11
0 0 1 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 1
0 0 0 1 0 0 0 0 1 0 0
0 1 0 1 1 0 0 0 1 0 0
0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 1 0 0 0
0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 1 0 0
0 0 0 0 0 0 1 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0

1
7
0 0 1 0 0 0 0
0 0 1 0 0 0 0
0 0 0 0 0 1 0
0 0 0 0 0 0 0
1 1 0 1 0 0 0
0 1 0 0 0 0 0
0 0 0 0 0 0 0

*/