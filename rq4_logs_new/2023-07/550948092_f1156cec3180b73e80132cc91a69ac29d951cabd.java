package BaekJoon.BFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

/*
    2667번 단지번호붙이기
 */
public class Solution {
    static int[][] arr;
    static int[] dx = {0, 1, 0, -1};
    static int[] dy = {1, 0, -1, 0};
    static boolean visited[][];
    static int n;
    static int num = 0;
    static int[] apartment = new int[25*25];
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        n = Integer.parseInt(br.readLine());
        arr = new int[n][n];
        for(int i = 0; i < n; i++){
            String s = br.readLine();
            for(int j = 0; j < n; j++){
                arr[i][j] = Integer.parseInt((s.substring(j, j+1)));
            }
        }
        visited = new boolean[n][n];
        for(int i = 0; i < n; i++){
            for(int j = 0; j < n; j++){
                if(arr[i][j] == 1 && !visited[i][j]) {
                    num++;
                    BFS(i, j);
                }
            }
        }
        Arrays.sort(apartment);
        System.out.println(num);

        for(int i = 0; i < apartment.length; i++){
            if(apartment[i] != 0)
                System.out.println(apartment[i]);
        }
    }
    static void BFS(int i, int j){
        Queue<int[]> queue = new LinkedList<>();
        queue.add(new int[]{i, j});
        visited[i][j] = true;
        apartment[num]++;

        while(!queue.isEmpty()){
            int[] now = queue.poll();
            int x = now[0];
            int y = now[1];
            for(int k = 0; k < 4; k++) {
                int nx = x + dx[k];
                int ny = y + dy[k];
                if(nx >= 0 && nx < n && ny >= 0 && ny < n){
                    if(arr[nx][ny] == 1 && !visited[nx][ny]){
                        queue.add(new int[]{nx, ny});
                        visited[nx][ny] = true;
                        apartment[num]++;
                    }
                }
            }
        }
    }
}