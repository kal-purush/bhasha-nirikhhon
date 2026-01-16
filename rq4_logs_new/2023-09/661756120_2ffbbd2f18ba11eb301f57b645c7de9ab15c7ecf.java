/*
Lv. 2 #155651 - 호텔 대실

    문제 설명
        호텔을 운영 중인 코니는 최소한의 객실만을 사용하여 예약 손님들을 받으려고 합니다. 한 번 사용한 객실은 퇴실 시간을 기준으로 10분간 청소를 하고 다음 손님들이 사용할 수 있습니다.
        예약 시각이 문자열 형태로 담긴 2차원 배열 book_time이 매개변수로 주어질 때, 코니에게 필요한 최소 객실의 수를 return 하는 solution 함수를 완성해주세요.


    제한사항
        · 1 ≤ book_time의 길이 ≤ 1,000
            · book_time[i]는 ["HH:MM", "HH:MM"]의 형태로 이루어진 배열입니다
                · [대실 시작 시각, 대실 종료 시각] 형태입니다.
            · 시각은 HH:MM 형태로 24시간 표기법을 따르며, "00:00" 부터 "23:59" 까지로 주어집니다.
                · 예약 시각이 자정을 넘어가는 경우는 없습니다.
                · 시작 시각은 항상 종료 시각보다 빠릅니다.


    입출력 예
        book_time	                                                                                                result
        [["15:00", "17:00"], ["16:40", "18:20"], ["14:20", "15:20"], ["14:10", "19:20"], ["18:20", "21:20"]]	       3
        [["09:10", "10:10"], ["10:20", "12:20"]]	                                                                   1
        [["10:20", "12:30"], ["10:20", "12:30"], ["10:20", "12:30"]]	                                               3


    입출력 예 설명
        입출력 예 #1
            ROOM 1      14:10           ~          19:20
            ROOM 2      14:20 ~ 15:20   16:40 ~ 18:20
            ROOM 3              15:00 ~ 17:00        18:20 ~ 21:20
            위 사진과 같습니다.

        입출력 예 #2
            첫 번째 손님이 10시 10분에 퇴실 후 10분간 청소한 뒤 두 번째 손님이 10시 20분에 입실하여 사용할 수 있으므로 방은 1개만 필요합니다.

        입출력 예 #3
            세 손님 모두 동일한 시간대를 예약했기 때문에 3개의 방이 필요합니다.
*/

/*
    정확성  테스트
        테스트 1 〉	통과 (1.63ms, 72.6MB)
        테스트 2 〉	통과 (2.91ms, 75.6MB)
        테스트 3 〉	통과 (7.10ms, 76.6MB)
        테스트 4 〉	통과 (4.11ms, 81.3MB)
        테스트 5 〉	통과 (1.04ms, 74.7MB)
        테스트 6 〉	통과 (4.60ms, 76MB)
        테스트 7 〉	통과 (6.28ms, 72.9MB)
        테스트 8 〉	통과 (3.79ms, 86.3MB)
        테스트 9 〉	통과 (3.60ms, 74.4MB)
        테스트 10 〉	통과 (5.87ms, 77.6MB)
        테스트 11 〉	통과 (6.58ms, 82.9MB)
        테스트 12 〉	통과 (7.96ms, 67.8MB)
        테스트 13 〉	통과 (3.01ms, 78.2MB)
        테스트 14 〉	통과 (6.85ms, 76.2MB)
        테스트 15 〉	통과 (6.74ms, 88.2MB)
        테스트 16 〉	통과 (3.32ms, 71.8MB)
        테스트 17 〉	통과 (6.06ms, 80.3MB)
        테스트 18 〉	통과 (4.36ms, 77.4MB)
        테스트 19 〉	통과 (3.80ms, 73.9MB)

    채점 결과
        정확성: 100.0
        합계: 100.0 / 100.0
*/

package C0012S;

import java.util.*;

class PRO155651 {
    public int solution(String[][] book_time) {
        PriorityQueue<int[]> priorityQueue = new PriorityQueue<>((o1, o2) -> o1[1] - o2[1]); // 대실 종료 시각을 기준으로 정렬
        Arrays.sort(book_time, (o1, o2) -> o1[0].compareTo(o2[0])); // 대실 시작 시각을 기준으로 정렬
        int roomNum = 0; // 코니에게 필요한 최소 객실의 수

        for (int b = 0; b < book_time.length; b++) {
            int start = Integer.parseInt(book_time[b][0].substring(0, 2)) * 60 + Integer.parseInt(book_time[b][0].substring(3)); // 분으로 변환한 대실 시작 시각
            int end = Integer.parseInt(book_time[b][1].substring(0, 2)) * 60 + Integer.parseInt(book_time[b][1].substring(3)) + 10; // 분으로 변환한 대실 종료 시각 + 청소 시간 10 분

            if (priorityQueue.isEmpty()) { // 우선 순위 큐가 비어 있을 경우
                roomNum += 1; // 객실의 수 추가
                priorityQueue.offer(new int[] {start, end});
                continue;
            }

            int prevTime[] = priorityQueue.poll(); // 우선 순위 큐의 첫 번째 원소
            if (start >= prevTime[1]) { // 대실 시작 시각이 우선 순위 큐의 첫 번째 원소의 종료 시간보다 클 경우
                priorityQueue.offer(new int[] {start, end});
            }
            else {
                roomNum += 1; // 객실의 수 추가
                priorityQueue.offer(new int[] {start, end});
                priorityQueue.offer(prevTime);
            }
        }

        return roomNum;
    }
}