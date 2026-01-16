package DoIt.MinimumSpanningTree;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/*
    065 다리 만들기
 */
public class Solution2_2 {
    static int[] dr = {-1, 0, 1, 0};
    static int[] dc = {0, 1, 0, -1};
    static int[] parent;
    static int[][] map;
    static int n,m, sNum;
    static boolean[][] visited;
    static ArrayList<ArrayList<int[]>> sumList; // 모든 섬 정보
    static ArrayList<int[]> mlist; // 1개의 섬 정보
    static PriorityQueue<bEdge> queue; // 다리 정보 저장할 우선순위 큐 - 모든 섬을 연결하는 다리가 각각 존재하지 않아도 됨
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer st = new StringTokenizer(br.readLine());
        n = Integer.parseInt(st.nextToken());
        m = Integer.parseInt(st.nextToken());
        map = new int[n][m];
        visited = new boolean[n][m];
        for(int i = 0; i < n; i++){
            st = new StringTokenizer(br.readLine());
            for(int j = 0; j < m; j++){
                map[i][j] = Integer.parseInt(st.nextToken());
            }
        }
        sNum = 1;
        sumList = new ArrayList<>();
        for(int i = 0; i < n; i++){ //각 자리에서 BFS 탐색으로 섬 분리
            for(int j = 0; j < m; j++) {
                if (map[i][j] != 0 && !visited[i][j]){ // 0(바다), 1(땅)
                    BFS(i, j);
                    sNum++;
                    sumList.add(mlist);
                }
            }
        }
        queue = new PriorityQueue<>();
        for(int i = 0; i < sumList.size(); i++){
            ArrayList<int[]> now = sumList.get(i);
            for(int j = 0; j < now.size(); j++){
                int r = now.get(j)[0];
                int c = now.get(j)[1];
                int now_S = map[r][c];
                for(int d = 0; d < 4; d++){
                    int tempR = dr[d];
                    int tempC = dc[d];
                    int blength = 0; //다리 길이 (2 이상이어야 함)
                    while(r + tempR >= 0 && r + tempR < n && c + tempC >= 0 && c + tempC < m){
                        if(map[r+tempR][c+tempC] == now_S) // 같은 섬이면 에지를 만들 수 없음
                            break;
                        else if(map[r+tempR][c+tempC] != 0){
                            if(blength > 1) //다른 섬
                                queue.add(new bEdge(now_S, map[r+tempR][c+tempC], blength));
                            break;
                        } else // 바다이면 다리 길이 연장
                            blength++;
                        //가로 세로 방향이 바뀌지 않고 계속 일직선으로 갈 수 있게 도와주는 장치
                        //가로가 음수이거나 양수일 때 세로는 무조건 0임을 기억하자
                        if(tempR < 0) tempR--;
                        else if(tempR > 0) tempR++;
                        else if(tempC < 0) tempC--;
                        else if(tempC > 0) tempC++;
                    }
                }
            }
        }
        parent = new int[sNum];
        for(int i = 0; i < parent.length; i++){
            parent[i] = i;
        }
        int useEdge = 0;
        int result = 0;
        while(!queue.isEmpty()){
            bEdge now = queue.poll();
            if(find(now.s) != find(now.e)){
                union(now.s, now.e);
                result += now.v;
                useEdge++;
            }
        }
        if(useEdge == sNum -2){
            System.out.println(result);
        } else{
            System.out.println("-1");
        }
    }
    public static void union(int a, int b){
        a = find(a);
        b = find(b);
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

    private static void BFS(int i, int j){
        Queue<int[]> queue = new LinkedList<>();
        mlist = new ArrayList<>();
        int[] start = {i, j};
        queue.add(start);
        mlist.add(start);
        visited[i][j] = true;
        map[i][j] = sNum;
        while(!queue.isEmpty()){
            int now[] = queue.poll();
            int r = now[0];
            int c = now[1];
            for(int d = 0; d < 4; d++){
                int tempR = dr[d];
                int tempC = dc[d];
                while(r + tempR >= 0 && r + tempR < n && c + tempC >= 0 && c + tempC < m){
                    if(!visited[r+tempR][c+tempC] && map[r+tempR][c+tempC] != 0){
                        addNode(r+tempR, c+tempC, queue);
                    } else break;
                    if(tempR < 0) tempR--;
                    else if(tempR > 0) tempR++;
                    else if(tempC < 0) tempC--;
                    else if(tempC > 0) tempC++;
                }
            }
        }
    }
    //특정 위치를 섬의 정보로 넣어 주는 함수
    private static void addNode(int i, int j, Queue<int[]> queue){
        map[i][j] = sNum;
        visited[i][j] = true;
        int[] temp = {i, j};
        mlist.add(temp);
        queue.add(temp);
    }
}