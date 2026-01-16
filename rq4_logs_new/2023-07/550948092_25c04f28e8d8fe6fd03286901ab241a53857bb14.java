package DoIt.BellmanFord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.StringTokenizer;

/*
    059 타임머신으로 빨리 가기
    벨만-포드 알고리즘
    음수 에지 가중치 허용할 때 사용
 */
public class Solution {
    static int n, m;
    static Edge edges[];
    static long distance[];
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        n = Integer.parseInt(st.nextToken());
        m = Integer.parseInt(st.nextToken());
        edges = new Edge[m+1];
        distance = new long[n+1];
        Arrays.fill(distance, Integer.MAX_VALUE);
        for(int i = 0; i < m; i++){
            st = new StringTokenizer(br.readLine());
            int start = Integer.parseInt(st.nextToken());
            int end = Integer.parseInt(st.nextToken());
            int time = Integer.parseInt(st.nextToken());
            edges[i] = new Edge(start, end, time);
        }
        distance[1] = 0;
        // n-1 만큼 반복 (출발 노드가 방문한 적 없으면 수행하지 않기 때문에 여러 번 반복해서 작업
        for(int i = 1; i < n; i++){
            for(int j = 0; j < m; j++){
                Edge edge = edges[j];
                //더 작은 최단 거리가 있으면 업데이트
                if(distance[edge.start] != Integer.MAX_VALUE
                    && distance[edge.end] > distance[edge.start] + edge.time){
                    distance[edge.end] = distance[edge.start] + edge.time;
                }
            }
        }
        boolean mCycle = false;
        //다시 돌면서 음수 사이클 있나 확인 - 만약 음수인 time이 있다면 다시 업데이트 발생
        for(int i = 0; i < m; i++) {
            Edge edge = edges[i];
            if(distance[edge.start] != Integer.MAX_VALUE
                    && distance[edge.end] > distance[edge.start] + edge.time) {
                mCycle = true;
            }
        }
        if(!mCycle){
            for(int i = 2; i <= n; i++){
                if(distance[i] == Integer.MAX_VALUE)
                    System.out.println("-1");
                else
                    System.out.println(distance[i]);
            }
        } else {
            System.out.println("-1");
        }
    }
    static class Edge {
        int start, end, time;

        public Edge(int start, int end, int time) {
            this.start = start;
            this.end = end;
            this.time = time;
        }
    }
}