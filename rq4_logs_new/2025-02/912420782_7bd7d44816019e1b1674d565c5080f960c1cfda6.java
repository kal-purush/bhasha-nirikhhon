/*
    다익스트라 알고리즘
    시작점
    distance배열 : 시작점을 기준 거리 배열
    found배열 : S에 포함되어 있는지 검사할 배열
    cost배열 : 모든 거리를 저장한 배열 -> 입력으로 주자.
*/

public class Dijkstra {
    //test할 main
    public static void main(String[] args) {
        //ppt예제 page47
        int[][] cost = {
                {0,50,10, 1000,45,1000},
                {1000,0,15,1000,10,1000},
                {20,1000,0,15, 1000,1000},
                {1000,20,1000,0,35,1000},
                {1000,1000,30,1000,0,1000},
                {1000,1000,1000,3,1000,0}
        };
        int[] distance = new int[6];
        boolean[] found = new boolean[6];
        for(int i = 0; i<6; i++) {
            distance[i] = cost[4][i];
            found[i] = false;
        }
        distance[4] = 0;

        dijkstra(4, cost, distance, 6, found);
        System.out.print("distance {");
        for(int i = 0; i<6; i++) {
            System.out.print(distance[i] + " ");
        }
        System.out.println("}");
    }
    public static void dijkstra(int v, int[][] cost, int[] distance, int n, boolean[] found) {
        found[v] = true;
        distance[v] = 0;
        for(int i = 0; i<n-1; i++) {
            int u = choose(distance, n, found);
            found[u] = true;
            for(int w = 0; w<n; w++) {
                if(!found[w] && cost[u][w] != 1000) {
                    if(distance[u] + cost[u][w] < distance[w])
                        distance[w] = distance[u] + cost[u][w];
                }
            }
        }
    }
    public static int choose(int[] distance, int n, boolean[] found) {
        int min = Integer.MAX_VALUE;
        int minpos = -1;
        for(int i = 0; i<n; i++) {
            if(distance[i] < min && !found[i]) {
                min = distance[i];
                minpos = i;
            }
        }
        return minpos;
    }
}