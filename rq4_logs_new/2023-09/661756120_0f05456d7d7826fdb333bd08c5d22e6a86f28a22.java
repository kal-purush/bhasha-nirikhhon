/*
2841. Silver 1 - 외계인의 기타 연주

    시간 제한	    메모리 제한        제출        정답	      맞힌 사람	    정답 비율
    1 초	    256 MB           9618	    4596      3516	         46.520%


    문제
        상근이의 상상의 친구 외계인은 손가락을 수십억개 가지고 있다. 어느 날 외계인은 기타가 치고 싶었고, 인터넷에서 간단한 멜로디를 검색했다. 이제 이 기타를 치려고 한다.
        보통 기타는 1번 줄부터 6번 줄까지 총 6개의 줄이 있고, 각 줄은 P개의 프렛으로 나누어져 있다. 프렛의 번호도 1번부터 P번까지 나누어져 있다.
        멜로디는 음의 연속이고, 각 음은 줄에서 해당하는 프렛을 누르고 줄을 튕기면 연주할 수 있다. 예를 들면, 4번 줄의 8번 프렛을 누르고 튕길 수 있다. 만약, 어떤 줄의 프렛을 여러 개 누르고 있다면, 가장 높은 프렛의 음이 발생한다.
        예를 들어, 3번 줄의 5번 프렛을 이미 누르고 있다고 하자. 이때, 7번 프렛을 누른 음을 연주하려면, 5번 프렛을 누르는 손을 떼지 않고 다른 손가락으로 7번 프렛을 누르고 줄을 튕기면 된다. 여기서 2번 프렛의 음을 연주하려고 한다면, 5번과 7번을 누르던 손가락을 뗀 다음에 2번 프렛을 누르고 연주해야 한다.
        이렇게 손가락으로 프렛을 한 번 누르거나 떼는 것을 손가락을 한 번 움직였다고 한다. 어떤 멜로디가 주어졌을 때, 손가락의 가장 적게 움직이는 회수를 구하는 프로그램을 작성하시오.


    입력
        첫째 줄에 멜로디에 포함되어 있는 음의 수 N과 한 줄에 있는 프렛의 수 P가 주어진다. (1 ≤ N ≤ 500,000, 2 ≤ P ≤ 300,000)
        다음 N개 줄에는 멜로디의 한 음을 나타내는 두 정수가 주어진다. 첫 번째 정수는 줄의 번호이고 두 번째 정수는 그 줄에서 눌러야 하는 프렛의 번호이다. 입력으로 주어진 음의 순서대로 기타를 연주해야 한다. 줄의 번호는 N보다 작거나 같은 자연수이고, 프렛의 번호도 P보다 작거나 같은 자연수이다.


    출력
        첫째 줄에 멜로디를 연주하는데 필요한 최소 손가락 움직임을 출력한다.


    예제 입력 1
        5 15
        2 8
        2 10
        2 12
        2 10
        2 5
    예제 출력 1
        7

    예제 입력 2
        7 15
        1 5
        2 3
        2 5
        2 7
        2 4
        1 5
        1 3
    예제 출력 2
        9


    알고리즘 분류
        자료 구조
        스택
*/


// 메모리 : 148368KB
// 시간 : 872ms
// 코드 길이 : 2882B
// 정답

package C0012S;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Stack;
import java.util.StringTokenizer;

public class BOJ2841 {
    public static void main(String[] args) throws IOException {
        BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
        StringTokenizer token = new StringTokenizer(bf.readLine());
        int N = Integer.parseInt(token.nextToken()); // 멜로디에 포함되어 있는 음의 수 N (1 ≤ N ≤ 500,000)
        int P = Integer.parseInt(token.nextToken()); // 한 줄에 있는 프렛의 수 P (2 ≤ P ≤ 300,000)

        Stack<int[]> stack[] = new Stack[N + 1]; // 1 차원 정수 배열을 원소로 갖는 스택 배열
        for (int s = 1; s <= N; s++) {
            stack[s] = new Stack<>(); // 각 줄마다 스택 생성
        }

        int minMoveNum = 0; // 멜로디를 연주하는 데 필요한 최소 손가락 움직임
        for (int n = 0; n < N; n++) {
            token = new StringTokenizer(bf.readLine());
            int line = Integer.parseInt(token.nextToken()); // 줄의 번호
            int fret = Integer.parseInt(token.nextToken()); // 해당 줄에서 눌러야 하는 프렛의 번호
            int nowTone[] = {line, fret}; // line 줄에서 fret 프렛을 누르고 줄을 튕겨서 나는 음

            if (stack[line].isEmpty() || stack[line].peek()[1] < fret) { // 해당 줄의 스택이 비어져 있거나, 해당 줄의 스택의 가장 위에 있는 항목의 프렛의 번호가 해당 줄에서 눌러야 하는 프렛의 번호보다 작을 경우
                stack[line].push(nowTone); // 해당 줄의 스택에 현재 연주해야 하는 정보 추가
                minMoveNum += 1; // 손가락 움직임 추가
            }
            else {
                while (!stack[line].isEmpty() && stack[line].peek()[1] > fret) { // 해당 줄의 스택이 비어져 있고, 해당 줄의 스택의 가장 위에 있는 항목의 프렛의 번호가 해당 줄에서 눌러야 하는 프렛의 번호보다 클 동안
                    stack[line].pop(); // 해당 줄의 스택에 있는 원소 제거
                    minMoveNum += 1; // 손가락 움직임 추가
                }

                if ((stack[line].isEmpty()) || (!stack[line].isEmpty() && stack[line].peek()[1] < fret)) { // 해당 줄의 스택이 비어져 있거나, 해당 줄의 스택이 비어져 있으며 해당 줄의 스택의 가장 위에 있는 항목의 프렛의 번호가 해당 줄에서 눌러야 하는 프렛의 번호보다 작을 경우
                    stack[line].push(nowTone); // 해당 줄의 스택에 현재 연주해야 하는 정보 추가
                    minMoveNum += 1; // 손가락 움직임 추가
                }
            }
        }

        System.out.println(minMoveNum);
    }
}