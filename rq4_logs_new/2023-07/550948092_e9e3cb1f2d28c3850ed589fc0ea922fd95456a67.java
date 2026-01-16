package DoIt.Graph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/*
    046 특정 거리의 도시 찾기
    복습
    BFS의 활용으로 최단 거리 찾기 - 연결선의 가중치가 모두 1이기 때문에(가중치가 없는 것과 마찬가지)
 */
public class Solution_2 {
    static ArrayList<Integer>[] arr;
    static int visited[];
    static int depth;
    static int k;
    static ArrayList<Integer>  ans;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        int n = Integer.parseInt(st.nextToken()); //도시 수
        int m = Integer.parseInt(st.nextToken()); //도로 개수
        k = Integer.parseInt(st.nextToken()); //거리 정보
        int x = Integer.parseInt(st.nextToken()); //출발 도시 번호
        arr = new ArrayList[n+1];
        for(int i = 1; i <= n; i++){
            arr[i] = new ArrayList<Integer>();
        }
        for(int i = 0; i < m; i++){
            st = new StringTokenizer(br.readLine());
            int s = Integer.parseInt(st.nextToken());
            int e = Integer.parseInt(st.nextToken());
            arr[s].add(e);
        }
        visited = new int[n+1];
        ans = new ArrayList<>();
        BFS(x);
        for(int i = 1; i <= n; i++){
            if(visited[i] == k+1){
                ans.add(i);
            }
        }
        if(ans.size() == 0){
            System.out.println("-1");
        } else {
            Collections.sort(ans);
            if(ans.iterator().hasNext()){
                System.out.print(ans.iterator().next() + " ");
            }
        }
    }
    static void BFS(int i){
        Queue<Integer> queue = new LinkedList<>();
        queue.add(i);
        visited[i] = 1;

        while(!queue.isEmpty()){
            int now = queue.poll();
            for(int n : arr[now]){
                if(visited[n] == 0) {
                    queue.add(n);
                    visited[n] = visited[now] + 1;
                }
            }
        }
    }
}