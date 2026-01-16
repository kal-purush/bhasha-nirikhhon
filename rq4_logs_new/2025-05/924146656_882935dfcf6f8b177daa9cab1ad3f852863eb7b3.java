package org.example.yhs3237.w14_tomato;

import java.util.*;
import java.io.*;

public class baekjoon7569 {
    static int M, N, H;
    static int[][][] tomatoes;
    // 델타배열 (위, 아래, 왼쪽, 오른쪽, 앞, 뒤)
    static int[] dz = {0, 0, 0, 0, -1, 1}; // 층 변화 (위, 아래)
    static int[] dx = {-1, 1, 0, 0, 0, 0}; // 행 변화 (상하)
    static int[] dy = {0, 0, -1, 1, 0, 0}; // 열 변화 (좌우)
    static Queue<int[]> queue;
    static int answer;

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        StringTokenizer st = new StringTokenizer(br.readLine());

        M = Integer.parseInt(st.nextToken()); // 가로 칸의 수
        N = Integer.parseInt(st.nextToken()); // 세로 칸의 수
        H = Integer.parseInt(st.nextToken()); // 쌓아올려지는 상자의 수

        queue = new LinkedList<>(); // 익은 토마토 저장

        // 상자의 상태 입력 받기 (1 : 익은 토마토, 0 : 익지 않은 토마토, -1 : 토마토가 들어있지 않은 칸)
        tomatoes = new int[H][N][M];

        for (int i = 0; i < H; i++) {
            for (int j = 0; j < N; j++) {
                st = new StringTokenizer(br.readLine());
                for (int k = 0; k < M; k++) {
                    tomatoes[i][j][k] = Integer.parseInt(st.nextToken());

                    // 처음부터 익어있던 토마토를 queue에 저장
                    if (tomatoes[i][j][k] == 1) {
                        queue.add(new int[] {i, j, k, 0});
                    }
                }
            }
        }

        boolean done = true; // 이미 모든 토마토가 익어있는지 여부
        for (int i = 0; i < H; i++) {
            for (int j = 0; j < N; j++) {
                for (int k = 0; k < M; k++) {
                    if (tomatoes[i][j][k] != 1) {
                        done = false; // 익지 않은 토마토가 있다면 false 반환
                    }
                }
                if (!done) break;
            }
            if (!done) break;
        }

        if (!done ) { // 익지 않은 토마토가 있다면 bfs 시행
            answer = 0; // 토마토가 모두 익는데 걸리는 시간
            bfs();

            boolean isAble = true; // bfs 시행 후 모든 토마토가 익을 수 있는지 여부
            for (int i = 0; i < H; i++) {
                for (int j = 0; j < N; j++) {
                    for (int k = 0; k < M; k++) {
                        if (tomatoes[i][j][k] == 0) isAble = false; // 익지 않은 토마토가 있다면 false 반환
                    }
                    if (!isAble) break;
                }
                if (!isAble) break;
            }

            // 익지 않은 토마토가 있다면 모두 익을 수 없으므로 -1 출력, 다 익었다면 익는데 걸리는 시간 출력
            if (!isAble) System.out.println(-1);
            else System.out.println(answer);
        } else { // 이미 모든 토마토가 익어있다면 bfs 시행하지 않고 0 출력
            System.out.println(0);
        }
    }

    static void bfs() {
        while (!queue.isEmpty()) {
            int[] current = queue.poll();
            int cx = current[0];
            int cy = current[1];
            int cz = current[2];
            int time = current[3];

            for (int i = 0; i < 6; i++) {
                int nx = cx + dx[i];
                int ny = cy + dy[i];
                int nz = cz + dz[i];

                if (inBox(nx, ny, nz) && tomatoes[nx][ny][nz] == 0) {
                    tomatoes[nx][ny][nz] = 1;
                    queue.add(new int[] {nx, ny, nz, time+1});
                }
            }

            // answer에 time 갱신
            answer = time;
        }
    }

    static boolean inBox(int z, int x, int y) {
        return (z >= 0 && z < H && x >= 0 && x < N && y >= 0 && y < M);
    }
}