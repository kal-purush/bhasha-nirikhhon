/*
Lv. 2 #148653 - 마법의 엘리베이터

    문제 설명
        마법의 세계에 사는 민수는 아주 높은 탑에 살고 있습니다. 탑이 너무 높아서 걸어 다니기 힘든 민수는 마법의 엘리베이터를 만들었습니다. 마법의 엘리베이터의 버튼은 특별합니다. 마법의 엘리베이터에는 -1, +1, -10, +10, -100, +100 등과 같이 절댓값이 10c (c ≥ 0 인 정수) 형태인 정수들이 적힌 버튼이 있습니다. 마법의 엘리베이터의 버튼을 누르면 현재 층 수에 버튼에 적혀 있는 값을 더한 층으로 이동하게 됩니다. 단, 엘리베이터가 위치해 있는 층과 버튼의 값을 더한 결과가 0보다 작으면 엘리베이터는 움직이지 않습니다. 민수의 세계에서는 0층이 가장 아래층이며 엘리베이터는 현재 민수가 있는 층에 있습니다.
        마법의 엘리베이터를 움직이기 위해서 버튼 한 번당 마법의 돌 한 개를 사용하게 됩니다.예를 들어, 16층에 있는 민수가 0층으로 가려면 -1이 적힌 버튼을 6번, -10이 적힌 버튼을 1번 눌러 마법의 돌 7개를 소모하여 0층으로 갈 수 있습니다. 하지만, +1이 적힌 버튼을 4번, -10이 적힌 버튼 2번을 누르면 마법의 돌 6개를 소모하여 0층으로 갈 수 있습니다.
        마법의 돌을 아끼기 위해 민수는 항상 최소한의 버튼을 눌러서 이동하려고 합니다. 민수가 어떤 층에서 엘리베이터를 타고 0층으로 내려가는데 필요한 마법의 돌의 최소 개수를 알고 싶습니다. 민수와 마법의 엘리베이터가 있는 층을 나타내는 정수 storey 가 주어졌을 때, 0층으로 가기 위해 필요한 마법의 돌의 최소값을 return 하도록 solution 함수를 완성하세요.


    제한사항
        · 1 ≤ storey ≤ 100,000,000


    입출력 예
        storey	    result
        16	        6
        2554	    16


    입출력 예 설명
        입출력 예 #1
            문제 예시와 같습니다.

        입출력 예 #2
            -1, +100이 적힌 버튼을 4번, +10이 적힌 버튼을 5번, -1000이 적힌 버튼을 3번 누르면 0층에 도착 할 수 있습니다. 그러므로 16을 return 합니다.
*/


/*
    정확성  테스트
        테스트 1 〉	통과 (0.12ms, 73.8MB)
        테스트 2 〉	통과 (0.05ms, 73MB)
        테스트 3 〉	통과 (1.63ms, 73.8MB)
        테스트 4 〉	통과 (0.08ms, 70.8MB)
        테스트 5 〉	통과 (0.12ms, 72MB)
        테스트 6 〉	통과 (0.11ms, 79.3MB)
        테스트 7 〉	통과 (0.09ms, 75.9MB)
        테스트 8 〉	통과 (0.14ms, 79.1MB)
        테스트 9 〉	통과 (0.10ms, 69.4MB)
        테스트 10 〉	통과 (0.11ms, 70.3MB)
        테스트 11 〉	통과 (0.10ms, 76.4MB)
        테스트 12 〉	통과 (0.10ms, 79.2MB)
        테스트 13 〉	통과 (0.06ms, 80.8MB)

    채점 결과
        정확성: 100.0
        합계: 100.0 / 100.0
*/


// 정답

package C0012S;

public class PRO148653 {
    public int calculateMovingButtonNum(String numString, int position, int magicStoneNum, int up) {
        if (position < 1) { // 일의 자리 수까지 모두 검사했을 경우
            return magicStoneNum + up;
        }

        int nowNum = numString.charAt(position - 1) - '0' + up; // 현재 자리 수 + 올림
        if (nowNum == 10) { // 현재 자리 수 + 올림의 값이 10일 경우, 다음 자리 수에 올림 추가
            return calculateMovingButtonNum(numString, position - 1, magicStoneNum, 1);
        }

        return Math.min(calculateMovingButtonNum(numString, position - 1, magicStoneNum + 10 - nowNum, 1), calculateMovingButtonNum(numString, position - 1, magicStoneNum + nowNum, 0)); // 현재 자리 수에서 위층으로 올라가 올림을 줘서 나오는 값과 현재 자리 수에서 아래층으로 내려가 나오는 값 중 최솟값
    }

    public int solution(int storey) {
        String storeyString = String.valueOf(storey); // 엘리베이터가 있는 층 문자열
        int positionNum = storeyString.length();

        return calculateMovingButtonNum(storeyString, positionNum, 0, 0);
    }
}