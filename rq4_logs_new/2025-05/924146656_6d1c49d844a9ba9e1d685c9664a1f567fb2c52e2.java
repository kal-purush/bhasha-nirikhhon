package org.example.yhs3237.w12_tetromino;

import java.io.*;
import java.util.*;

public class baekjoon14500 {
    static int N, M;
    static int[][] paper;
    static boolean[][] visited;
    static int max = Integer.MIN_VALUE;
    // 델타배열 (상우하좌)
    static int[] dr = {-1, 0, 1, 0};
    static int[] dc = {0, 1, 0, -1};

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        StringTokenizer st = new StringTokenizer(br.readLine());

        N = Integer.parseInt(st.nextToken()); // 종이의 세로 크기
        M = Integer.parseInt(st.nextToken()); // 종이의 가로 크기

        // 초기화 및 입력 받기
        paper = new int[N][M];
        visited = new boolean[N][M];

        for (int i = 0; i < N; i++) {
            st = new StringTokenizer(br.readLine());
            for (int j = 0; j < M; j++) {
                paper[i][j] = Integer.parseInt(st.nextToken());
            }
        }

        // 각 좌표마다 dfs + dfs로 탐색 안 되는 경우 검사
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                visited[i][j] = true;
                dfs(i, j, 1, paper[i][j]); // 일반적인 모양 탐색
                visited[i][j] = false; // 방문배열을 dfs실행 시 재사용하기 위한 clear처리
                checkExtraShape(i, j); // ㅗ, ㅜ, ㅓ, ㅏ 모양 탐색
            }
        }

        System.out.println(max);
    }

    static void dfs(int r, int c, int depth, int sum) {
        if (depth == 4) {
            max = Math.max(max, sum);
            return;
        }

        for (int d = 0; d < 4; d++) {
            int nr = r + dr[d];
            int nc = c + dc[d];

            if (onPaper(nr, nc) && !visited[nr][nc]) {
                visited[nr][nc] = true;
                dfs(nr, nc, depth + 1, sum + paper[nr][nc]);
                visited[nr][nc] = false;
            }
        }
    }

    static void checkExtraShape(int r, int c) {
        // 중심에서 3방향을 뻗는 모양 (ㅗ, ㅜ, ㅓ, ㅏ)
        for (int i = 0; i < 4; i++) {
            int sum = paper[r][c];
            boolean valid = true;

            for (int j = 0; j < 3; j++) {
                int dir = (i + j) % 4;
                int nr = r + dr[dir];
                int nc = c + dc[dir];

                if (!onPaper(nr, nc)) {
                    valid = false;
                    break;
                }
                sum += paper[nr][nc];
            }

            if (valid) {
                max = Math.max(max, sum);
            }
        }
    }

    // 범위 체크
    static boolean onPaper(int r, int c) {
        return (r >= 0 && r < N && c >= 0 && c < M);
    }
}