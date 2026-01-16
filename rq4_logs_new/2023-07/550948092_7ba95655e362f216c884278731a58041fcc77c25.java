package DoIt.DynamicProgramming;

import java.io.IOException;
import java.util.Scanner;

/*
    095 외판원의 순회 경로 짜기
 */
public class Solution7 {
    private static final int INF = 1000000 * 16 + 1;
    private static int n;
    private static int[][] w;
    private static int[][] d; // 점화식 배열
    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(System.in);
        n = sc.nextInt();
        w = new int[16][16];
        for(int i = 0; i < n; i++) {
            for(int j = 0; j < n; j++){
                w[i][j] = sc.nextInt();
            }
        }
        d = new int[16][1 << 16];
        for(int i = 0; i < n; i++){
            for(int j = 0; j < 1 << n; j++){
                d[i][j] = INF;
            }
        }
        System.out.println(tsp(0, 1));
    }
    private static int tsp(int c, int v){ // 모든 경우의 수 완전 탐색하고 재귀 구현
        if(v == (1 << n) - 1){ // 모든 노드 방문할 때
            return w[c][0] == 0 ? INF  : w[c][0];
        }
        if(d[c][v] != INF){ // 이미 방문한 노드 -> 바로 리턴(메모이제이션)
            return d[c][v];
        }
        for(int i = 0; i < n; i++){
            if((v & (1 << i)) == 0 && w[c][i] != 0){ // 방문한 적 없고 갈 수 있을 때
                d[c][v] = Math.min(d[c][v], tsp(i, (v | (1<<i))) + w[c][i]);
            }
        }
        return d[c][v];
    }
}
/*
    #TSP (외판원 순환 문제)
    n의 크기가 작을 때 모든 순서를 완전 탐색 수행해 정답을 구할 수 있음

    ##점화식
    D[c][v]: 현재 도시 c, 지금까지 모든 도시 리스트가 V일 때
    앞으로 남은 모든 도시를 경유하는 데 필요한 최소 비용
    방문하지 않은 도시를 i라고 할 때
    = Math.main(D[c][v], D[c][v] | (1 << i)] + W[c][i])

    ##비트 연산식
    AND: 두 수의 각자리수에 대해 둘 다 1일 경우 1로 변환
    OR: 두 수의 각자리수가 하나만 1이면 1로 변환
    XOR: 두 수의 각 자리수가 다른 경우 1로 변환
    NOT: 각 자리수가 반대로 변환
    SHIFT:
        <<: 이진수의 각자리를 왼쪽으로 N칸 밀고 제일 첫째 자리 0으로 채움
            [예] 3 << 2 = 3 * 2^2 = 12
            3의 2진수 011 -> 2칸 왼쪽으로 밀면 1100 -> 맨 앞 자리에 0으로 채워 01100 = 12
        >>: 이진수의 각자리를 오른쪽으로 N칸 밀고 밀면서 사라지는 자리수 삭제
            [예] 16 >> 3 = 16 / 2^3 = 2
            16의 2진수 10000 -> 뒤의 3칸 제거 10 -> 제거된 자릿수 만큼 앞에서부터
            양수이면 0으로, 음수이면 1으로 채워 00010
            (>>>는 항상 0으로 채워 양수만 취급)
 */