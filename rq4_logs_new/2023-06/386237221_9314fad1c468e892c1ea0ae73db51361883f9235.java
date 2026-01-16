package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

// https://www.acmicpc.net/problem/2186
// 문자판
public class Main {
    private static int N, M, K;
    private static char[][] board;
    private static char[] target;
    private static int[][][] dp;


    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        N = Integer.parseInt(st.nextToken());
        M = Integer.parseInt(st.nextToken());
        K = Integer.parseInt(st.nextToken());

        board = new char[N][M];
        for (int i = 0; i < N; i++) {
            board[i] = br.readLine().toCharArray();
        }

        target = br.readLine().toCharArray();
        dp = new int[N][M][target.length];
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                Arrays.fill(dp[i][j], -1);
            }
        }

        int answer = 0;
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                if (board[i][j] == target[0]) {
                    answer += dfs(i, j, 0);
                }
            }
        }

        System.out.println(answer);
    }

    static int dfs(int x, int y, int depth) {
        if (dp[x][y][depth] != -1) { // 이전에 탐색했으면 추가 탐색 X
            return dp[x][y][depth];
        }

        if (depth == target.length - 1) { // depth 최대치에 도달한 경우
            return dp[x][y][depth] = 1;
        }

        dp[x][y][depth] = 0;

        int[] dx = {-1, 1, 0, 0};
        int[] dy = {0, 0, -1, 1};
        for (int j = 0; j < 4; j++) { // 상하좌우로 K만큼 탐색
            for (int k = 1; k <= K; k++) {
                int nx = x + dx[j] * k;
                int ny = y + dy[j] * k;
                if (nx < 0 || nx >= N || ny < 0 || ny >= M) {
                    continue;
                }
                if (board[nx][ny] == target[depth + 1]) {
                    dp[x][y][depth] += dfs(nx, ny, depth + 1);
                }
            }
        }
        return dp[x][y][depth];
    }
}
/*
Input
4 4 1
KAKT
XEAS
YRWU
ZBQP
BREAK

Output
3
*/