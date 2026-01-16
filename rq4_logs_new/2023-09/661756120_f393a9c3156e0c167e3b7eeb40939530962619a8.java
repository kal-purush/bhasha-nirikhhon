/*
15900. Silver 1 - 나무 탈출

    시간 제한	                메모리 제한        제출        정답	      맞힌 사람	    정답 비율
    2 초 (추가 시간 없음)	    512 MB           7614	    2239      1705	         36.706%


    문제
        평소에 사이가 좋지 않던 성원이와 형석이가 드디어 제대로 한 판 붙으려고 한다. 성원이와 형석이 둘과 모두 똑같이 친한 인섭이가 대결 종목을 정해 가져왔다. 바로 '나무 탈출' 이라는 보드게임이다.
        '나무 탈출' 은 N개의 정점이 있는 트리 모양으로 생긴 게임판과 몇 개의 게임말로 이루어진다. 트리의 각 정점에는 1번부터 N번까지 번호가 붙어있다. 1번 정점은 '루트 노드' 라고 불리며, 이 루트 노드를 중심으로 정점 간에 부모-자식 관계가 만들어진다. 자식이 없는 노드는 '리프 노드' 라고 불린다.
        이 게임은 두 사람이 번갈아 가면서 게임판에 놓여있는 게임말을 움직이는 게임이다. 처음에는 트리의 모든 리프 노드에 게임말이 하나씩 놓여있는 채로 시작한다. 어떤 사람의 차례가 오면, 그 사람은 현재 존재하는 게임말 중 아무거나 하나를 골라 그 말이 놓여있던 노드의 부모 노드로 옮긴다. 이 과정에서 한 노드에 여러 개의 게임말이 놓이게 될 수도 있다. 이렇게 옮긴 후에 만약 그 게임말이 루트 노드에 도착했다면 그 게임말을 즉시 제거한다. 모든 과정을 마치면 다음 사람에게 차례를 넘긴다. 이런 식으로 계속 진행하다가 게임말이 게임판에 존재하지 않아 고를 수 없는 사람이 지게 된다.
        성원이를 얕본 형석이는 쿨하게 이 게임의 선을 성원이에게 줘버렸다. 따라서 성원이가 먼저 시작하고 형석이가 나중에 시작한다. 그동안 형석이와 대결을 하면 매번 지기만 했던 성원이는 마음속에 분노가 가득 쌓였다. 이번 대결에서는 반드시 이겨서 형석이의 코를 꺾어주고 싶다. 그래서 게임을 시작하기 전에 게임판의 모양만 보고 이 게임을 이길 수 있을지 미리 알아보고 싶어졌다. 성원이가 이 게임을 이길 수 있을지 없을지를 알려주는 프로그램을 만들어 성원이를 도와주자.


    입력
        첫째 줄에 트리의 정점 개수 N(2 ≤ N ≤ 500,000)이 주어진다.
        둘째 줄부터 N-1줄에 걸쳐 트리의 간선 정보가 주어진다. 줄마다 두개의 자연수 a, b(1 ≤ a, b ≤ N, a ≠ b)가 주어지는데, 이는 a와 b 사이에 간선이 존재한다는 뜻이다.


    출력
        성원이가 최선을 다했을 때 이 게임을 이길 수 있으면 Yes, 아니면 No를 출력한다.


    예제 입력 1
        2
        2 1
    예제 출력 1
        Yes

    예제 입력 2
        4
        1 2
        2 3
        2 4
    예제 출력 2
        No

    예제 입력 3
        8
        8 1
        1 4
        7 4
        6 4
        6 5
        1 3
        2 3
    예제 출력 3
        No


    알고리즘 분류
        그래프 이론
        그래프 탐색
        트리
        깊이 우선 탐색
*/


// 메모리 : 201720KB
// 시간 : 1592ms
// 코드 길이 : 2105B
// 정답

package C0012S;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class BOJ15900 {
    static ArrayList<Integer> tree[]; // 트리 모양으로 생긴 게임판  // 게임판의 어떤 위치에 게임말이 있는지 저장하는 ArrayList형 배열
    static int moveNum; // 전체 게임말의 이동 횟수

    public static void moveNode(int index, int parent, int count) { // 게임말을 이동하는 함수  // moveNode(
        if (tree[index].size() == 1 && tree[index].get(0) == parent) { // 연결된 노드가 하나만 있고, 연결된 노드가 부모 노드일 경우
            moveNum += count; // 루트 노드이므로 전체 게임말의 이동 횟수에 현재 게임말의 이동 횟수를 더한다.
            return;
        }

        for (int t = 0; t < tree[index].size(); t++) {
            if (tree[index].get(t) == parent) {
                continue;
            }

            moveNode(tree[index].get(t), index, count + 1);
        }
    }

    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        int N = Integer.parseInt(bf.readLine()); // 트리의 정점 개수 N (2 ≤ N ≤ 500,000)

        tree = new ArrayList[N + 1];
        for (int i = 1; i < N + 1; i++) {
            tree[i] = new ArrayList<>();
        }

        for (int n = 0; n < N - 1; n++) { // 트리의 간선 정보
            StringTokenizer token = new StringTokenizer(bf.readLine());
            int a = Integer.parseInt(token.nextToken());
            int b = Integer.parseInt(token.nextToken());

            tree[a].add(b);
            tree[b].add(a);
        }

        moveNode(1, -1, 0);
        if (moveNum % 2 == 0) { // 게임말의 이동 횟수가 짝수일 경우
            System.out.println("No"); // 성원이가 진다.
        }
        else { // 게임말의 이동 횟수가 홀수일 경우
            System.out.println("Yes"); // 성원이가 이긴다.
        }
    }
}