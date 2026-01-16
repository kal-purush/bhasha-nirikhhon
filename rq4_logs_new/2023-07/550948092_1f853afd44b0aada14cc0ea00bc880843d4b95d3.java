package DoIt.BFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Queue;
import java.util.StringTokenizer;

public class Solution2_2 {
    static int[][] arr;
    static boolean[][] visited;
    static int n, m;
    static int[] dx = {0, 1, 0, -1};
    static int[] dy = {1, 0, -1, 0};
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        n = Integer.parseInt(st.nextToken());
        m = Integer.parseInt(st.nextToken());
        arr = new int[n+1][m+1];
        for(int i = 1; i <= n; i++){
            String s = br.readLine();
            char[] ch = s.toCharArray();
            for(int j = 1; j <= m; j++){
                arr[i][j] = ch[j-1] - '0';
            }
        }
        visited = new boolean[n+1][m+1];
        BFS(1, 1);
        System.out.println(arr[n][m]);
    }
    static void BFS(int i, int j){
        Queue<Node> queue = new LinkedList<>();
        queue.add(new Node(i, j));
        while(!queue.isEmpty()){
            Node now = queue.poll();
            visited[i][j] = true;
            for(int r = 0; r < 4; r++){
                int x = now.i + dx[r];
                int y = now.j + dy[r];
                if(x >= 0 && y >= 0 && x <= n && y <= m){
                    if(arr[x][y] != 0 && !visited[x][y]){
                        visited[x][y] = true;
                        arr[x][y] = arr[now.i][now.j] + 1;
                        queue.add(new Node(x, y));
                    }
                }
            }
        }

    }

    static class Node {
        int i;
        int j;

        public Node(int i, int j) {
            this.i = i;
            this.j = j;
        }
    }
}