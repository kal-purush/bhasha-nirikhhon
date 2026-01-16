package ybwi0912;

import java.util.*;

/*
* 2023-09-05
* PRO 155651번: 호텔 대실
* 1. 입력받은 String 배열을 int 배열로 변환한다. 이때 퇴실 시각 + 청소 시간 10분을 반영해 값을 조정한다
* 2. 람다식을 사용해 int 배열을 오름차순으로 정렬한다. 대실 시작 시각을 우선적으로 비교하고, 같다면 대실 종료 시각을 비교한다.
* 3. 우선순위 큐를 사용해 각 예약을 객실에 배정한다.
* - 우선순위 큐 내의 데이터 하나를 객실 하나라고 가정하고, 각 객실의 마지막 퇴실 시각을 저장한다.
* - int 배열에 저장된 데이터들을 정렬된 순서대로 객실의 마지막 퇴실 시각과 비교한다.
*       - 같은 객실에 예약할 수 있다면, 즉 만약 마지막 퇴실 시각과 새 데이터의 시작 시각이 겹치지 않으면 큐에서 기존 데이터를 제거하고 새 데이터의 퇴실 시각을 추가한다.
*       - 같은 객실에 예약할 수 없다면, 즉 만약 마지막 퇴실 시각과 새 데이터의 시작 시각이 겹치면 기존 데이터 제거 없이 큐에 새 데이터의 퇴실 시각을 추가한다.
* 4. 모든 객실의 배정을 마치고, 객실의 개수를 최종적으로 반환한다. 여기서 객실의 개수는 우선순위 큐 내에 들어 있는 데이터의 개수와 같다.
* */

class Solution {
    public int solution(String[][] book_time) {
        // 1. 입력받은 배열을 int 배열로 변환
        int[][] times = new int[book_time.length][2];
        for (int i = 0; i < book_time.length; i++) {
            times[i][0] = Integer.parseInt(book_time[i][0].replace(":", ""));
            int e = Integer.parseInt(book_time[i][1].replace(":", "")) + 10; // 퇴실 후 청소 시간 반영

            if (e % 100 >= 60) { // 청소 시간 10분을 더했을 때 60분 이상이라면
                e = e - 60 + 100; // 60분 빼고 한 시간 추가
            }

            times[i][1] = e;
        }

        // 2. 오름차순으로 정렬
        Arrays.sort(times, (a, b) -> {
            if (a[0] > b[0]) return 1;
            else if (a[0] < b[0]) return -1; // 대실 시작 시각을 우선적으로 비교
            else {
                if (a[1] >= b[1]) return 1; // 시작 시각이 같다면 종료 시각을 비교
                else return -1;
            }
        });

        // 3. 각 예약을 객실에 배정
        PriorityQueue<Integer> rooms = new PriorityQueue<>();
        // 요소를 추가하면 자동으로 오름차순 정렬이 되는 priorityQueue를 사용
        // int[]를 추가하려고 했으나 굳이 시작 시각과 종료 시각을 모두 가지고 있을 필요가 없다고 생각해 종료 시각만 저장
        // queue에 들어 있는 각 요소들이 객실이라고 생각

        for (int[] book : times) {
            if (rooms.isEmpty()) { // 첫 예약의 퇴실 시각을 큐에 넣음
                rooms.add(book[1]);
            } else { // 두 번째 예약부터
                int end = rooms.peek(); // 예약된 객실 중 가장 늦은 퇴실 시각을 가져온다

                if (end <= book[0]) { // 같은 객실에 예약할 수 있다면
                    rooms.poll(); // 기존 데이터를 제거하고 (같은 객실로 취급하기 위해)
                    rooms.add(book[1]); // 새 데이터의 종료 시각을 추가한다.
                } else { // 같은 객실에 예약할 수 없다면
                    rooms.add(book[1]); // 기존 데이터를 제거하지 않고 그 위에 새 데이터의 종료 시각을 추가
                }
            }
        }

        return rooms.size(); // 우선순위 큐의 사이즈(= 객실의 총 개수)를 반환한다
    }
}

public class PRO155651 {
    public static void main(String[] args) {
        Solution solution = new Solution();

        String[][] book_time = {{"15:00", "17:00"}, {"16:40", "18:20"}, {"14:20", "15:20"}, {"14:10", "19:20"}, {"18:20", "21:20"}};

        int ans = solution.solution(book_time);

        System.out.println(ans);
    }
}