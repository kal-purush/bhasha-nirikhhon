package DoIt.Dijkstra;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

/*
    056 최단 경로 구하기
 */
public class Solution6 {
    public static int v, e, k;
    public static int distance[];
    public static boolean visited[];
    public static ArrayList<Edge> list[];
    public static PriorityQueue<Edge> q = new PriorityQueue<>();
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        v = Integer.parseInt(st.nextToken());
        e = Integer.parseInt(st.nextToken());
        k = Integer.parseInt(br.readLine());
        list = new ArrayList[v+1];
        visited = new boolean[v+1];
        distance = new int[v+1];
        for(int i = 0; i <= v; i++){
            list[i] = new ArrayList<Edge>();
        }
        for(int i = 0; i < e; i++){
            st = new StringTokenizer(br.readLine());
            int s = Integer.parseInt(st.nextToken());
            int e = Integer.parseInt(st.nextToken());
            int v = Integer.parseInt(st.nextToken());
            list[s].add(new Edge(e, v));
        }
        q.add(new Edge(k, 0));
        distance[k] = 0;
        while(!q.isEmpty()){
            Edge cur = q.poll();
            int cur_v = cur.end;
            if(visited[cur_v]) continue;
            visited[cur_v] = true;
            for(int i = 0; i < list[cur_v].size(); i++){
                Edge tmp = list[cur_v].get(i);
                int next = tmp.end;
                int value = tmp.value;
                if(distance[next] > distance[cur_v] + value){
                    distance[next] = value + distance[cur_v];
                    q.add(new Edge(next, distance[next]));
                }
            }
            for(int i = 1; i <= v; i++){
                if(visited[i])
                    System.out.println(distance[i]);
                else
                    System.out.println("INF");
            }
        }
    }
    static class Edge implements Comparable<Edge>{
        //int start;
        int end;
        int value;

        public Edge(int end, int value) {
            //this.start = start;
            this.end = end;
            this.value = value;
        }
        public int compareTo(Edge e){
            if(this.value > e.value) return 1;
            else return -1;
        }
    }
}