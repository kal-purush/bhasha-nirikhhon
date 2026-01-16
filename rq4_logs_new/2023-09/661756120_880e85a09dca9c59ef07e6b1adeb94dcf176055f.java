package Jieun714;
/**
 * 문제: '나무 탈출' 은 N개의 정점이 있는 트리 모양으로 생긴 게임판과 몇 개의 게임말로 이루어진다. 트리의 각 정점에는 1번부터 N번까지 번호가 붙어있다. 1번 정점은 '루트 노드' 라고 불리며, 이 루트 노드를 중심으로 정점 간에 부모-자식 관계가 만들어진다. 자식이 없는 노드는 '리프 노드' 라고 불린다.
 *      이 게임은 두 사람이 번갈아 가면서 게임판에 놓여있는 게임말을 움직이는 게임이다. 처음에는 트리의 모든 리프 노드에 게임말이 하나씩 놓여있는 채로 시작한다. 어떤 사람의 차례가 오면, 그 사람은 현재 존재하는 게임말 중 아무거나 하나를 골라 그 말이 놓여있던 노드의 부모 노드로 옮긴다.
 *      이 과정에서 한 노드에 여러 개의 게임말이 놓이게 될 수도 있다. 이렇게 옮긴 후에 만약 그 게임말이 루트 노드에 도착했다면 그 게임말을 즉시 제거한다. 모든 과정을 마치면 다음 사람에게 차례를 넘긴다. 이런 식으로 계속 진행하다가 게임말이 게임판에 존재하지 않아 고를 수 없는 사람이 지게 된다.
 *      성원이를 얕본 형석이는 쿨하게 이 게임의 선을 성원이에게 줘버렸다. 따라서 성원이가 먼저 시작하고 형석이가 나중에 시작한다. 그동안 형석이와 대결을 하면 매번 지기만 했던 성원이는 마음속에 분노가 가득 쌓였다. 이번 대결에서는 반드시 이겨서 형석이의 코를 꺾어주고 싶다.
 *      그래서 게임을 시작하기 전에 게임판의 모양만 보고 이 게임을 이길 수 있을지 미리 알아보고 싶어졌다. 성원이가 이 게임을 이길 수 있을지 없을지를 알려주는 프로그램을 만들어 성원이를 도와주자.
 *
 * 입력: 첫째 줄에 트리의 정점 개수 N(2 ≤ N ≤ 500,000)이 주어진다.
 *      둘째 줄부터 N-1줄에 걸쳐 트리의 간선 정보가 주어진다. 줄마다 두개의 자연수 a, b(1 ≤ a, b ≤ N, a ≠ b)가 주어지는데, 이는 a와 b 사이에 간선이 존재한다는 뜻이다.
 *
 * 출력: 성원이가 최선을 다했을 때 이 게임을 이길 수 있으면 Yes, 아니면 No를 출력한다.
 *
 * 해결: 리프 노드의 깊이의 누적 합을 구해 나머지로 결과를 계산
 * */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class BOJ15900 {
    public static int N, total;
    public static ArrayList<Integer> [] graph;
    public static boolean [] isVisited;

    //bfs - 인자값 현재노드와 현재 깊이
    public static void bfs(int current, int depth){
        Queue<int[]> queue = new ArrayDeque<>();
        queue.add(new int[] {current, depth});
        isVisited[current] = true; //방문 체크

        while(!queue.isEmpty()){ //큐가 비지 않을 때까지
            int [] now = queue.poll();
            boolean flag = true; //리프노드를 체크하기 위한 boolean 변수

            for(int nextNode : graph[now[0]]){ //현재 노드와 인접노드 탐색
                if(isVisited[nextNode]) continue; //방문했으면 다음 노드로
                flag = false; //리프노드가 아님을 체크
                isVisited[nextNode] = true; //방문 체크
                queue.add(new int[]{nextNode, now[1]+1}); //다음 노드와 깊이+1
            } //for end

            if(flag){ //리프노드라면
                total += now[1]; //깊이를 누적
            }
        } //while end
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        N = Integer.parseInt(br.readLine()); //트리의 정점 개수
        graph = new ArrayList[N+1]; //간선 정보를 담을 리스트
        for(int i=0; i<N+1; i++){
            graph[i] = new ArrayList<>();
        }

        for(int i=0; i<N-1; i++){
            StringTokenizer st = new StringTokenizer(br.readLine());
            int a = Integer.parseInt(st.nextToken());
            int b = Integer.parseInt(st.nextToken());
            graph[a].add(b);
            graph[b].add(a);
        }
        isVisited = new boolean[N+1]; //방문 여부를 체크할 배열
        bfs(1,0);

        // 홀수면 성원 승, 짝수면 형석 승
        System.out.println(total%2==0 ? "No":"Yes");
    }
}