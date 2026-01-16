package ybwi0912;

import java.util.Scanner;

/*
 * 2023-09-19
 * PRO 148653번: 마법의 엘리베이터
 * [ 문제 요약 ]
 * 민수와 마법의 엘리베이터가 있는 층을 나타내는 storey가 주어진다. 마법의 엘리베이터는 한 번에 절댓값 10^c만큼 이동할 수 있고, 버튼 한 번당 마법의 돌 한 개를 사용한다.
 * 단, 엘리베이터가 위치해 있는 층과 버튼의 값을 더한 결과가 0보다 작으면 엘리베이터는 움직이지 않는다.
 * 민수가 어떤 층에서 엘리베이터를 타고 0층으로 내려가는데 필요한 마법의 돌의 최소 개수를 return한다.
 * [ 제한사항 ]
 * 1 <= storey <= 100,000,000
 * [ 입출력 예 ]
 *   storey  |  result
 *     16    |    6
 *    2554   |   16
 * */

public class PRO148653 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int storey = sc.nextInt();

        int ans = solution(storey);

        System.out.println(ans);
    }

    static int solution(int storey) { // storey 층에서 0층으로 내려가는 데 필요한 마법의 돌의 최소 개수 answer
        int answer = 0;

        // 작은 자릿수부터 차례대로 정리. 매번 나머지를 계산하면 번거로움이 있으니 배열에 각 자리의 수를 저장
        // int형의 배열로 저장하기 위해 int -> String -> char[] -> int[]의 전환 과정을 거친다
        String s = String.valueOf(storey);
        char[] c = s.toCharArray();
        int[] input = new int[c.length];

        for(int i=0; i<c.length; i++){
            input[i] = c[i] - 48;
        }

        for(int i=input.length - 1; i >= 0; i--){
            int now = input[i];

            if(now >= 6){ // 해당 자릿수가 6, 7, 8, 9라면 0이 될 때까지 +1을 해주는 것이 이득
                answer += (10 - now);
                if(i-1 < 0){
                    answer++;
                } else {
                    input[i-1]++;
                }
            }
            else if(now <= 4){ // 해당 자릿수가 1, 2, 3, 4라면 0이 될 때까지 -1을 해주는 것이 이득
                answer += now;
            }
            else if(now == 5){ // 해당 자릿수가 5라면 다음 자릿수를 보고 판단
                if(i-1 < 0){ // 해당 자릿수가 마지막 자릿수라면
                    answer += now;
                } else{
                    if(input[i-1] < 5){
                        answer += now;
                    } else {
                        answer += now;
                        input[i-1]++;
                    }
                }
            }
        }

        return answer;
    }
}