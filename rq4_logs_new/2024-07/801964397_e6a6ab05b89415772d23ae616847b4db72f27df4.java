package no16173;

import java.io.*;
import java.util.*;


public class Main {
    //빈공간 찾는 방법 오른쪽 아래
    static int[] dx = {1, 0};
    static int[] dy = {0, 1};
    //map 저장
    static int[][] map;
    //방문확인 배열
    static boolean[][] visited;
    //배열의 크기
    static int n;
    static Queue<int[]> queue = new LinkedList<>();

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st;

        n = Integer.parseInt(br.readLine());
        map = new int[n][n];    // n x n 크기의 map
        visited = new boolean[n][n];

        //정사각형의 칸 안에 값 넣기
        for (int i = 0; i < map.length; i++) {
            st = new StringTokenizer(br.readLine(), " ");
            for (int j = 0; j < map[0].length; j++) {
                map[i][j] = Integer.parseInt(st.nextToken());
            }
        }
        //queue에 시작점 넣음
        queue.add(new int[]{0,0});
        visited[0][0] = true;
        bfs();

    }

    public static void bfs(){
        while (!queue.isEmpty()){
            int[] tmp = queue.poll(); //큐의 값 꺼내요
            int x = tmp[0];
            int y = tmp[1];
            if (map[x][y] == -1){ //map의 마지막점의 값은 -1이므로 끝 점까지 도착한 경우
                System.out.println("HaruHaru");
                return;
            }
            for (int i = 0; i < 2; i++) {
                //map의 값만큼 이동하므로 *map[x][y]
                int nx = x + dx[i]*map[x][y];
                int ny = y + dy[i]*map[x][y];
                if (nx < n && ny < n && !visited[nx][ny]){
                    //map안에 있고, 방문하지 않은 점일때
                    queue.add(new int[]{nx, ny});
                    visited[nx][ny] = true;
                }
            }
        }
        //도달하지 못했을 때
        System.out.println("Hing");
    }
}