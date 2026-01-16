package ybwi0912;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/*
 * 2023-09-07
 * BOJ 15900번: 나무 탈출
 * 트리를 그렸을 때, 모든 리프 노드의 depth의 합이 홀수이면 성원이가 이기고(Yes), 합이 짝수이면 형석이가 이긴다(No).
 * tree 내의 각 ArrayList들이 트리의 각 노드들이라고 설정.
 * 방향이 없는 그래프이기 때문에 양쪽 노드에 서로를 추가해준 다음에 DFS로 루트 노드부터 전체 노드를 방문하면서 각 리프 노드의 depth를 구해 sum에 합산한다
 * */

public class BOJ15900 {
    static List<ArrayList<Integer>> tree = new ArrayList<>();
    static boolean[] isVisited; // 각 노드의 방문 여부 체크하기 위한 배열
    static int sum = 0; // 모든 리프 노드의 depth의 합

    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer token = new StringTokenizer(bf.readLine());

        int N = Integer.parseInt(token.nextToken()); // 트리의 정점의 개수
        isVisited = new boolean[N+1];

        for(int i=0; i<=N; i++)
            tree.add(new ArrayList<>());


        for(int i=0; i<N-1; i++){
            token = new StringTokenizer(bf.readLine());
            int a = Integer.parseInt(token.nextToken());
            int b = Integer.parseInt(token.nextToken());
            tree.get(a).add(b);
            tree.get(b).add(a);
            // 방향이 없는 그래프이기 때문에 양쪽에 추가
        }
        // input

        dfs(1, 0);
        // operation

        if(sum%2!=0) System.out.println("Yes"); // 합이 홀수일 경우 승리
        else System.out.println("No"); // 합이 짝수일 경우 패배
        // output
    }

    static void dfs(int vertex, int depth){
        isVisited[vertex] = true; // 해당 정점 방문 처리
        for(int next: tree.get(vertex)){ // 해당 정점에서 방문할 수 있는 모든 정점들에 대해서
            if(!isVisited[next]){ // 다음 정점에 방문하지 않았다면
                dfs(next, depth+1); // depth 카운트를 1 더한 값을 가지고 다음 노드로 이동
            }
        }

        if(tree.get(vertex).size()==1 && vertex != 1){ // 해당 노드가 부모 노드 하나만을 가지고 있는 자식 노드일 경우 재귀를 종료.
            // 이때 해당 노드가 자식 노드를 하나만 가진 루트 노드일 경우를 꼭 제외해야 한다
            sum += depth; // depth 합산
        }
    }
}