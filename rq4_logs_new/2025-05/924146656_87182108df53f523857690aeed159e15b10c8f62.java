package org.example.yhs3237.w13_searchCourse;

import java.io.*;
import java.util.*;

public class baekjoon1504 {
    static int N, E;
    static List<Node>[] connected;

    static class Node implements Comparable<Node> {
        int index, cost;

        Node(int index, int cost) {
            this.index = index;
            this.cost = cost;
        }

        @Override
        public int compareTo(Node o) {
            return this.cost - o.cost; // 오름차순 (Dijkstra용)
        }
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());

        N = Integer.parseInt(st.nextToken()); // 정점 수
        E = Integer.parseInt(st.nextToken()); // 간선 수

        // 인접 리스트 초기화
        connected = new ArrayList[N + 1];
        for (int i = 1; i <= N; i++) {
            connected[i] = new ArrayList<>();
        }

        // 간선 정보 입력
        for (int i = 0; i < E; i++) {
            st = new StringTokenizer(br.readLine());
            int a = Integer.parseInt(st.nextToken());
            int b = Integer.parseInt(st.nextToken());
            int c = Integer.parseInt(st.nextToken());

            connected[a].add(new Node(b, c));
            connected[b].add(new Node(a, c));
        }

        st = new StringTokenizer(br.readLine());
        int v1 = Integer.parseInt(st.nextToken()); // 거쳐야 할 정점1
        int v2 = Integer.parseInt(st.nextToken()); // 거쳐야 할 정점2

        // 두 가지 경로 모두 계산
        int path1 = dijkstra(1, v1);
        int path2 = dijkstra(v1, v2);
        int path3 = dijkstra(v2, N);

        int path4 = dijkstra(1, v2);
        int path5 = dijkstra(v2, v1);
        int path6 = dijkstra(v1, N);

        // 도달 불가한 경우 체크
        if (path1 == Integer.MAX_VALUE || path2 == Integer.MAX_VALUE || path3 == Integer.MAX_VALUE ||
                path4 == Integer.MAX_VALUE || path5 == Integer.MAX_VALUE || path6 == Integer.MAX_VALUE) {
            System.out.println(-1);
        } else {
            long result1 = (long) path1 + path2 + path3;
            long result2 = (long) path4 + path5 + path6;
            System.out.println(Math.min(result1, result2));
        }
    }

    // 다익스트라 알고리즘
    static int dijkstra(int from, int to) {
        int[] cost = new int[N + 1];
        Arrays.fill(cost, Integer.MAX_VALUE);
        cost[from] = 0;

        PriorityQueue<Node> pq = new PriorityQueue<>();
        pq.offer(new Node(from, 0));

        while (!pq.isEmpty()) {
            Node cur = pq.poll();

            if (cost[cur.index] < cur.cost) continue;

            for (Node next : connected[cur.index]) {
                if (cost[next.index] > cur.cost + next.cost) {
                    cost[next.index] = cur.cost + next.cost;
                    pq.offer(new Node(next.index, cost[next.index]));
                }
            }
        }

        return cost[to];
    }
}