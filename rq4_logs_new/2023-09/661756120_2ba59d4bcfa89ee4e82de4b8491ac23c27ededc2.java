/*
11729. Gold 5 - 하노이 탑 이동 순서

    시간 제한	    메모리 제한        제출        정답	      맞힌 사람	    정답 비율
    1 초	    256 MB           80663	    41096     31447	         50.473%


    문제
        세 개의 장대가 있고 첫 번째 장대에는 반경이 서로 다른 n개의 원판이 쌓여 있다. 각 원판은 반경이 큰 순서대로 쌓여있다. 이제 수도승들이 다음 규칙에 따라 첫 번째 장대에서 세 번째 장대로 옮기려 한다.
            1. 한 번에 한 개의 원판만을 다른 탑으로 옮길 수 있다.
            2. 쌓아 놓은 원판은 항상 위의 것이 아래의 것보다 작아야 한다.

        이 작업을 수행하는데 필요한 이동 순서를 출력하는 프로그램을 작성하라. 단, 이동 횟수는 최소가 되어야 한다.
        아래 그림은 원판이 5개인 경우의 예시이다.
             1                                                   1
             2                                                   2
             3                          →                        3
             4                                                   4
             5                                                   5
            [1]     [2]     [3]                 [1]     [2]     [3]


    입력
        첫째 줄에 첫 번째 장대에 쌓인 원판의 개수 N (1 ≤ N ≤ 20)이 주어진다.


    출력
        첫째 줄에 옮긴 횟수 K를 출력한다.
        두 번째 줄부터 수행 과정을 출력한다. 두 번째 줄부터 K개의 줄에 걸쳐 두 정수 A B를 빈칸을 사이에 두고 출력하는데, 이는 A번째 탑의 가장 위에 있는 원판을 B번째 탑의 가장 위로 옮긴다는 뜻이다.


    예제 입력 1
        3
    예제 출력 1
        7
        1 3
        1 2
        3 2
        1 3
        2 1
        2 3
        1 3


    알고리즘 분류
        재귀
*/


// 메모리 : 47200KB
// 시간 : 524ms
// 코드 길이 : 1475B
// 정답

package C0012S;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class BOJ11729 {
    static StringBuilder sb = new StringBuilder();

    public static void HanoiTower(int n, int first, int second, int third) { // 하노이 탑 진행 과정을 나타낸 함수  // HanoiTower(원판의 개수, 출발 위치, 도착 위치에 도달하기 전 거쳐가는 위치, 도착 위지)
        if (n == 1) { // 이동할 원판의 개수가 1 개일 경우
            sb.append(first).append(" ").append(third).append("\n");
            return;
        }

        HanoiTower(n - 1, first, third, second); // n - 1 번째까지의 원판을 첫 번째 위치에서 두 번째 위치로 이동
        sb.append(first).append(" ").append(third).append("\n"); // n 번째 원판을 첫 번째 위치에서 세 번째 위치로 이동
        HanoiTower(n - 1, second, first, third); // (n - 1) 개의 원판을 모두 두 번째 위치에서 세 번째 위치로 이동
    }

    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        int N = Integer.parseInt(bf.readLine()); // 첫 번째 장대에 쌓인 원판의 개수 N (1 ≤ N ≤ 20)

        sb.append((int) (Math.pow(2, N) - 1)).append("\n"); // 하노이 탑의 최소 이동 횟수는 N 층일 경우, 2^N - 1

        HanoiTower(N, 1, 2, 3);

        System.out.println(sb);
    }
}