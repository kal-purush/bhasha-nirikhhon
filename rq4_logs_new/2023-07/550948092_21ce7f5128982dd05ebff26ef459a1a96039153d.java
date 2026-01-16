package DoIt.Greedy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

/*
    035 회의실 배정하기
 */
public class Solution2_2 {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        int[][] arr = new int[n][2];
        for(int i = 0; i < n; i++){
            StringTokenizer st = new StringTokenizer(br.readLine());
            arr[i][0] = Integer.parseInt(st.nextToken());
            arr[i][1] = Integer.parseInt(st.nextToken());
        }
        Arrays.sort(arr, new Comparator<int[]>() {
            @Override
            public int compare(int[] S, int[] E) {
                if(S[1] == E[1]){ //종료 시간 같다면 시작 시간이 빠른 것부터
                    return S[0] - E[0];
                }
                return S[1] - E[1]; //종료 시간 빠른 것부터
            }
        });
        int cnt = 0;
        int end = -1;
        for(int i = 0; i < n; i++){
            if(arr[i][0] >= end){
                end = arr[i][1];
                cnt++;
            }
        }
        System.out.println(cnt);
    }
}

/*
    # 비교

    ### Comparable

    - 기본 정렬 기준으로 구현
    - 오름차순
    - compareTo(매개변수)로 나를 기준으로 매개변수와 비교
        - 나 > 매개변수 - 양수 반환
        - 나 == 매개변수 - 0 반환
        - 나 < 매개변수 - 음수 반환

    ### Comparator

    - 기본 정렬 기준 외 다른 기준으로 정렬
    - 오름차순
    - 파라미터로 들어오는 두 매개변수 객체 비교
    - compare(매개1, 매개2)
        - 매개1 > 매개2 - 양수 반환
        - 매개1 == 매개2 - 0 반환
        - 매개1 < 매개2 - 음수 반환`
 */