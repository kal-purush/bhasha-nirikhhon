package DoIt.MinimumSpanningTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/*
    064 최소 신장 트리
    union-find, parent 배열 정석 문제
 */
public class Solution_2 {
    static int[] parent;
    static PriorityQueue<Edge> list;
    static boolean visited[];
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        int n = Integer.parseInt(st.nextToken());
        int m = Integer.parseInt(st.nextToken());
        parent = new int[n+1];
        for(int i = 0; i < n; i++){
            parent[i] = i;
        }
        list = new PriorityQueue<>();
        for(int i = 0; i < m; i++){
            st = new StringTokenizer(br.readLine());
            int s = Integer.parseInt(st.nextToken());
            int e = Integer.parseInt(st.nextToken());
            int v = Integer.parseInt(st.nextToken());
            list.add(new Edge(s, e, v));
        }
        int useEdge = 0;
        int result = 0;
        while(useEdge < n-1){
            pEdge now = queue.poll();
            if(find(now.s) != find(now.e)){ //같은 부모가 아니라면 연결해도 사이틀 안생김
                union(now.s, now.e);
                result = result + now.v;
                useEdge++;
            }
        }
        System.out.println(result);
    }

    public static void union(int a, int b){
        int a = find(a);
        int b = find(b);
        if(a != b){
            parent[b] = a;
        }
    }

    public static int find(int a){
        if(a == parent[a])
            return a;
        else
            return parent[a] = find(parent[a]);
    }

    static class Edge implements Comparable<Edge>{
        int s;
        int e;
        int v;

        public Edge(int s, int e, int v) {
            this.s = s;
            this.e = e;
            this.v = v;
        }

        @Override
        public int compareTo(Edge o) {
            return this.v - o.v;
        }
    }
}