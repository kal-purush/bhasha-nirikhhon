package DoIt.DFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

/*
`   Problem 023
    DFS 복습
 */
public class Solution4_2 {
    static ArrayList<Integer>[] arr;
    static boolean visited[];
    static int turn;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        int n = Integer.parseInt(st.nextToken());
        int m = Integer.parseInt(st.nextToken());
        arr = new ArrayList[n+1];
        for(int i = 1; i <= n; i++){
            arr[i] = new ArrayList<Integer>();
        }
        for(int i = 0; i < m; i++) {
            st = new StringTokenizer(br.readLine());
            int s = Integer.parseInt(st.nextToken());
            int e = Integer.parseInt(st.nextToken());
            arr[s].add(e);
            arr[e].add(s);
        }
        visited = new boolean[n+1];
        int answer = 0;
        for(int i = 1; i <= n; i++) {
            turn = 0;
            DFS(i);
            if(turn != 0) answer++;
        }
        System.out.println(answer);
    }

    static void DFS(int n){
        if(visited[n] == true){
            return;
        }
        visited[n] = true;
        turn++;
        for(int i : arr[n]){
            if(!visited[i]){
                DFS(i);
            }
        }
    }
}