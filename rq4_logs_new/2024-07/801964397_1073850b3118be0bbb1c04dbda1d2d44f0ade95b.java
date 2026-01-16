
/*
3
1 1 10
1 5 1
2 2 -1
--
HaruHaru
*/

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


public class Main {

    static boolean isGoal = false;
    public static void main(String[] args) throws Exception{
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        int N = Integer.parseInt(br.readLine());

        int[][] map = new int[N][N];
        boolean[][] visited = new boolean[N][N];

        for(int i=0; i<N; i++){
            StringTokenizer st = new StringTokenizer(br.readLine(), " ");
            for (int j = 0; j < N; j++) {
                map[i][j] = Integer.parseInt(st.nextToken());
            }
        }
        dfs(0, 0, N, map,visited);
        if(isGoal){
            System.out.println("HaruHaru");
        }else{
            System.out.println("Hing");
        }

    }

    static void dfs(int x, int y, int num, int[][] map, boolean[][] visited){
        if(visited[x][y] == false){
            visited[x][y] = true;

            if(map[x][y] == -1){
                isGoal = true;
                return;
            }

            //down
            if(x + map[x][y] > 0 && x + map[x][y] < num){
                dfs(x + map[x][y], y, num, map, visited);
            }

            //right
            if(y + map[x][y] > 0 && y + map[x][y] < num){
                dfs(x, y + map[x][y], num, map, visited);
            }
        }else{
            return;
        }

    }
}