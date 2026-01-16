package org.example.rt8632.w14_Tomato;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

public class boj_7569 {
    static int[] dx = { 1, 0, -1, 0, 0, 0 };
    static int[] dy = { 0, 1, 0, -1, 0, 0 };
    static int[] dz = { 0, 0, 0, 0, 1, -1 };
    static int[][][] box;
    static boolean[][][] visited;
    static int M,N,H;
    static Queue<int[]> q = new LinkedList<>();

    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());

        M = Integer.parseInt(st.nextToken());
        N = Integer.parseInt(st.nextToken());
        H = Integer.parseInt(st.nextToken());

        box = new int[N][M][H];
        visited = new boolean[N][M][H];

        for (int k = 0; k < H; k++) {
            for (int i = 0; i < N; i++) {
                st = new StringTokenizer(br.readLine());
                for (int j = 0; j < M; j++) {
                    box[i][j][k] = Integer.parseInt(st.nextToken());
                }
            }
        }

        boolean flag = false;
        for (int k = 0; k < H; k++) {
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < M; j++) {
                    if(box[i][j][k] == 0)
                        flag = true;
                }
            }
        }

        //모든 토마토가 익어있는 상태
        if(!flag) {
            System.out.println(0);
            return;
        }



        for (int k = 0; k < H; k++) {
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < M; j++) {
                    if(box[i][j][k] == 1)
                        q.add(new int[] {i,j,k});
                }
            }
        }

        bfs();

        flag = false;
        for (int k = 0; k < H; k++) {
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < M; j++) {
                    if(box[i][j][k] == 0) {
                        flag = true;
                        break;
                    }
                }
            }
        }

        if (flag) {
            System.out.println(-1);
            return;
        }

        int max = -1;
        for (int k = 0; k < H; k++) {
            for (int i = 0; i < N; i++) {
                for (int j = 0; j < M; j++) {
                    if(box[i][j][k] > max)
                        max = box[i][j][k];
                }
            }
        }
        System.out.println(max-1);
    }

    static void bfs() {
        while(!q.isEmpty()) {
            int[] cur = q.poll();
            int cx = cur[0];
            int cy = cur[1];
            int cz = cur[2];

            visited[cx][cy][cz] = true;
            for(int i=0;i<6;i++) {
                int nx = cx + dx[i];
                int ny = cy + dy[i];
                int nz = cz + dz[i];

                if(nx < 0 || ny < 0 || nz < 0 || nx >= N || ny >= M || nz >= H)
                    continue;
                if(box[nx][ny][nz] == -1  || box[nx][ny][nz] == 1 || visited[nx][ny][nz])
                    continue;
                visited[nx][ny][nz] = true;
                box[nx][ny][nz] = box[cx][cy][cz] + 1;
                q.add(new int[] {nx,ny,nz});
            }
        }
    }
}