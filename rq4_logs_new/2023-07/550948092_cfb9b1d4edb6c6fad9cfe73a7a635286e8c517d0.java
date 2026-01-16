package DoIt.Graph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

/*
    048 이분 그래프 판별하기
    같은 집합에 속한 정점끼리는 간선이 존재해서는 안됨!
 */
public class Solution3_2 {
    static ArrayList<Integer>[] arr;
    static boolean visited[];
    static int[] check;
    static boolean isEven;
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int k = Integer.parseInt(br.readLine());
        for(int i = 0; i < k; i++) {
            StringTokenizer st = new StringTokenizer(br.readLine());
            int v = Integer.parseInt(st.nextToken());
            int e = Integer.parseInt(st.nextToken());
            arr = new ArrayList[v+1];
            for(int j = 0; j <= v; j++){
                arr[j] = new ArrayList<>();
            }
            for(int j = 0; j < e; j++){
                st = new StringTokenizer(br.readLine());
                int f = Integer.parseInt(st.nextToken());
                int t = Integer.parseInt(st.nextToken());
                arr[f].add(t);
                arr[t].add(f);
            }
            visited = new boolean[v+1];
            check = new int[v+1];
            isEven = true;
            for(int j = 1; j <= v; j++) {
                if(isEven)
                    DFS(j);
                else
                    break;
            }
            check[1] = 0;
            if(isEven) System.out.println("YES");
            else System.out.println("NO");
        }
    }

    static void DFS(int i){
        visited[i] = true;
        for(int n : arr[i]){
            if(!visited[n]) {
                //인접한 노드는 같은 집합이 아니므로 이전 노드와 다른 집합으로 처리
                check[i] = (check[i] + 1) % 2;
                DFS(n);
            }
            //이미 방문한 노드라면 현재 노드와 같은 집합인지 확인 -> 같으면 이분 그래프 아님
            else if(check[n] == check[i])
                isEven = false;
        }
    }
}